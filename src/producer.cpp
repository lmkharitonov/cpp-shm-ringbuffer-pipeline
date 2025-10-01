#include "ring_buffer.h"
#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <zstd.h>
#include <iomanip>

// --- Глобальные переменные и структуры ---

struct PacketHeader {
    int32_t block_index;
    uint16_t fragment_index;
    uint16_t total_fragments;
    uint32_t data_size;
};

// ... (остальные глобальные переменные workQueue, resultQueue и т.д. остаются) ...

// --- Функции-воркеры и вспомогательные функции ---

void worker(bool use_compression) {
    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(workMutex);
            workCv.wait(lock, [] { return !workQueue.empty() || readingComplete; });

            if (workQueue.empty() && readingComplete) {
                break;
            }

            item = std::move(workQueue.front());
            workQueue.pop();
        }

        size_t original_size = item.data.size();

        if (use_compression) {
            size_t maxDstSize = ZSTD_compressBound(original_size);
            std::vector<uint8_t> compressed_data(maxDstSize);
            size_t compressed_size = ZSTD_compress(compressed_data.data(), maxDstSize, item.data.data(), original_size, 1);

            if (ZSTD_isError(compressed_size)) {
                std::cerr << "ZSTD compression failed for block " << item.index << std::endl;
                continue;
            }
            compressed_data.resize(compressed_size);
            item.data = std::move(compressed_data);
        }

        {
            std::lock_guard<std::mutex> lock(resultMutex);
            resultQueue.push({item.index, std::move(item.data), original_size});
        }
        resultCv.notify_one();
    }
}

SharedLayout* setup_shm(int& shm_fd) {
    shm_unlink(SHM_NAME);
    shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) throw std::runtime_error("shm_open failed");

    if (ftruncate(shm_fd, SHM_SIZE) == -1) {
        close(shm_fd);
        shm_unlink(SHM_NAME);
        throw std::runtime_error("ftruncate failed");
    }

    void* shm_ptr = mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) {
        close(shm_fd);
        shm_unlink(SHM_NAME);
        throw std::runtime_error("mmap failed");
    }
    
    auto* shared_layout = static_cast<SharedLayout*>(shm_ptr);
    shared_layout->head.store(0);
    shared_layout->tail.store(0);
    shared_layout->total_source_bytes.store(0);
    
    return shared_layout;
}

size_t enqueue_source_blocks(const char* filepath, SharedLayout* shared_layout) { 
    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file) throw std::runtime_error("Failed to open input file");

    uint64_t total_source_bytes = file.tellg();
    shared_layout->total_source_bytes.store(total_source_bytes);
    file.seekg(0, std::ios::beg);

    size_t blockIndex = 0;
    while (file) {
        std::vector<uint8_t> buffer(RAW_BLOCK_FLOAT_COUNT * sizeof(float));
        file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        size_t bytesRead = file.gcount();
        if (bytesRead == 0) break;
        buffer.resize(bytesRead);

        {
            std::lock_guard<std::mutex> lock(workMutex);
            workQueue.push({static_cast<int>(blockIndex++), std::move(buffer)});
        }
        workCv.notify_one();
    }
    return blockIndex;
}

uint64_t send_compressed_blocks(RingBuffer& ringBuffer, size_t total_blocks) {
    uint64_t total_transmitted_bytes = 0;
    size_t blocks_processed = 0;

    while (blocks_processed < total_blocks) {
        ResultItem item;
        {
            std::unique_lock<std::mutex> lock(resultMutex);
            resultCv.wait(lock, [] { return !resultQueue.empty(); });
            item = std::move(resultQueue.front());
            resultQueue.pop();
        }

        const size_t max_payload_size = ringBuffer.getCapacity() - sizeof(PacketHeader) - 1;
        const size_t total_fragments = (item.data.size() + max_payload_size - 1) / max_payload_size;

        for (size_t i = 0; i < total_fragments; ++i) {
            PacketHeader header;
            header.block_index = item.index;
            header.fragment_index = i;
            header.total_fragments = total_fragments;

            size_t offset = i * max_payload_size;
            header.data_size = std::min(max_payload_size, item.data.size() - offset);

            ringBuffer.write(reinterpret_cast<uint8_t*>(&header), sizeof(header));
            ringBuffer.write(item.data.data() + offset, header.data_size);

            total_transmitted_bytes += sizeof(header) + header.data_size;
        }
        blocks_processed++;
    }
    return total_transmitted_bytes;
}

void cleanup(void* shm_ptr, int shm_fd) {
    if (shm_ptr != MAP_FAILED) {
        munmap(shm_ptr, SHM_SIZE);
    }
    if (shm_fd != -1) {
        close(shm_fd);
    }
    // Не вызываем shm_unlink здесь, Consumer должен это делать.
}


/**
 * @brief Основная функция "Producer".
 */
void producer(const char* filepath, bool use_compression) {
    int shm_fd = -1;
    SharedLayout* shared_layout = setup_shm(shm_fd);
    RingBuffer ringBuffer(shared_layout);

    // Запуск рабочих потоков.
    unsigned int num_threads = std::thread::hardware_concurrency(); //std::thread::hardware_concurrency() - возвращает количество так называемых аппаратных потоков (hardware thread contexts)
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < num_threads; ++i) {
        workers.emplace_back(worker, use_compression);
    }
    
    // Чтение файла и получение общего числа блоков.
    // Это блокирующая операция, но после нее основной поток сразу начнет обработку.
    const size_t total_blocks = enqueue_source_blocks(filepath, shared_layout);
    
    // **ВАЖНО**: Получаем значение до освобождения памяти.
    const uint64_t total_source_bytes = shared_layout->total_source_bytes.load();

    // Сигнал рабочим потокам, что чтение файла завершено.
    readingComplete = true;
    workCv.notify_all();

    // **ОПТИМИЗАЦИЯ**: Основной поток немедленно начинает обрабатывать результаты,
    // не дожидаясь полного завершения всех воркеров.
    uint64_t total_transmitted_bytes = send_compressed_blocks(ringBuffer, total_blocks);

    // Дожидаемся завершения всех рабочих потоков.
    for (auto& worker_thread : workers) {
        worker_thread.join();
    }

    // Отправка маркера конца передачи.
    PacketHeader end_marker = {-1, 0, 0, 0};
    ringBuffer.write(reinterpret_cast<uint8_t*>(&end_marker), sizeof(end_marker));

    // --- Вывод метрик (ПЕРЕД очисткой) ---
    std::cout << "\n--- Producer Metrics ---" << std::endl;
    std::cout << "Mode: " << (use_compression ? "COMPRESSED" : "UNCOMPRESSED") << std::endl;
    std::cout << "Total source data: " << total_source_bytes << " bytes" << std::endl;
    std::cout << "Total transmitted data: " << total_transmitted_bytes << " bytes" << std::endl;
    if (use_compression && total_source_bytes > 0) {
        double ratio = static_cast<double>(total_transmitted_bytes) / total_source_bytes * 100.0;
        std::cout << "Compression Ratio (transmitted/source): " << std::fixed << std::setprecision(2) << ratio << "%" << std::endl;
    }

    // Очистка ресурсов
    cleanup(shared_layout, shm_fd);
}

// --- Точка входа (main) ---
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <inputfile> [--nocompress]\n";
        return 1;
    }

    bool use_compression = true;
    if (argc > 2 && std::string(argv[2]) == "--nocompress") {
        use_compression = false;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    try {
        producer(argv[1], use_compression);
    } catch (const std::exception& e) {
        std::cerr << "Error in producer: " << e.what() << '\n';
        shm_unlink(SHM_NAME);
        return 1;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "Producer total time: " << duration.count() << " ms\n";
    
    return 0;
}
