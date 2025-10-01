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
#include <map>
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

// Структура для сборки фрагментированных блоков.
struct FragmentedBlock {
    std::vector<std::vector<uint8_t>> fragments;
    size_t received_fragments = 0;
    size_t total_size = 0;

    FragmentedBlock(size_t total) { fragments.resize(total); }
};

// Контейнер для готовых к записи блоков, отсортированных по индексу.
std::map<int, std::vector<float>> final_blocks;
std::mutex final_mutex;
std::condition_variable final_cv;
int next_block_to_write = 0;
std::atomic<bool> writer_should_finish = false;

// --- Функции-воркеры и основная логика ---

/**
 * @brief Функция рабочего потока для декомпрессии данных.
 *
 * Извлекает полные блоки из `workQueue`, выполняет декомпрессию (если нужно)
 * и помещает результат в `final_blocks` для последующей записи.
 * @param use_compression Флаг, указывающий, нужно ли выполнять декомпрессию.
 */
void decompression_worker(bool use_compression) {
    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(workMutex);
            workCv.wait(lock, [] { return !workQueue.empty() || readingComplete; });

            if (workQueue.empty() && readingComplete) break;

            item = std::move(workQueue.front());
            workQueue.pop();
        }

        std::vector<float> final_data;
        if (use_compression) {
            unsigned long long const rSize = ZSTD_getFrameContentSize(item.data.data(), item.data.size());
            if (rSize == ZSTD_CONTENTSIZE_ERROR || rSize == ZSTD_CONTENTSIZE_UNKNOWN) {
                continue; // Ошибка в данных, пропускаем.
            }
            final_data.resize(rSize / sizeof(float));
            ZSTD_decompress(final_data.data(), rSize, item.data.data(), item.data.size());
        } else {
            final_data.resize(item.data.size() / sizeof(float));
            memcpy(final_data.data(), item.data.data(), item.data.size());
        }

        {
            std::lock_guard<std::mutex> lock(final_mutex);
            final_blocks[item.index] = std::move(final_data);
        }
        final_cv.notify_one(); // Уведомляем поток записи о новом готовом блоке.
    }
}

/**
 * @brief Поток для последовательной записи данных в файл.
 *
 * Ожидает появления в `final_blocks` блока с нужным индексом (`next_block_to_write`),
 * записывает его в файл и инкрементирует индекс.
 * @param outputFile Поток для записи в выходной файл.
 * @param total_bytes_written Ссылка на счетчик записанных байт.
 */
void writer_thread(std::ofstream& outputFile, uint64_t& total_bytes_written) {
    while (true) {
        std::unique_lock<std::mutex> lock(final_mutex);
        // Ожидаем, пока появится следующий по порядку блок или придет сигнал о завершении.
        final_cv.wait(lock, [] { return final_blocks.count(next_block_to_write) > 0 || writer_should_finish; });

        // Записываем все доступные последовательные блоки.
        while (final_blocks.count(next_block_to_write) > 0) {
            auto& data_to_write = final_blocks.at(next_block_to_write);
            size_t bytes_to_write = data_to_write.size() * sizeof(float);
            outputFile.write(reinterpret_cast<const char*>(data_to_write.data()), bytes_to_write);
            total_bytes_written += bytes_to_write;

            final_blocks.erase(next_block_to_write);
            next_block_to_write++;
        }

        // Если пришел сигнал завершения и все блоки записаны, выходим.
        if (writer_should_finish && final_blocks.empty()) {
            break;
        }
    }
}

/**
 * @brief Подключается к существующей разделяемой памяти.
 * 
 * @param shm_fd Ссылка на файловый дескриптор.
 * @return Указатель на SharedLayout в SHM.
 */
SharedLayout* connect_to_shm(int& shm_fd) {
    // Пытаемся подключиться к SHM несколько раз с задержкой.
    for (int i = 0; i < 20; ++i) {
        shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
        if (shm_fd != -1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (shm_fd == -1) {
        throw std::runtime_error("shm_open failed after multiple attempts");
    }

    void* shm_ptr = mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) {
        close(shm_fd);
        throw std::runtime_error("mmap failed");
    }
    return static_cast<SharedLayout*>(shm_ptr);
}


/**
 * @brief Читает данные из буфера, собирает блоки и отправляет их на обработку.
 * 
 * @param ringBuffer Кольцевой буфер для чтения.
 */
void enqueue_compressed_blocks(RingBuffer& ringBuffer) {
    std::map<int, FragmentedBlock> fragment_map;
    
    while (true) {
        PacketHeader header;
        ringBuffer.read(reinterpret_cast<uint8_t*>(&header), sizeof(header));

        // Маркер конца передачи.
        if (header.block_index == -1) break;

        std::vector<uint8_t> fragment_data(header.data_size);
        ringBuffer.read(fragment_data.data(), header.data_size);

        // Если это первый фрагмент блока, создаем для него запись в карте.
        if (fragment_map.find(header.block_index) == fragment_map.end()) {
            fragment_map.emplace(header.block_index, FragmentedBlock(header.total_fragments));
        }

        auto& block = fragment_map.at(header.block_index);
        if (block.fragments[header.fragment_index].empty()) {
            block.fragments[header.fragment_index] = std::move(fragment_data);
            block.received_fragments++;
            block.total_size += header.data_size;
        }

        // Если все фрагменты блока получены, собираем их в единый кусок данных.
        if (block.received_fragments == header.total_fragments) {
            std::vector<uint8_t> full_data;
            full_data.reserve(block.total_size);
            for (const auto& frag : block.fragments) {
                full_data.insert(full_data.end(), frag.begin(), frag.end());
            }

            // Отправляем собранный блок в очередь на декомпрессию.
            {
                std::lock_guard<std::mutex> lock(workMutex);
                workQueue.push({header.block_index, std::move(full_data)});
            }
            workCv.notify_one();
            fragment_map.erase(header.block_index);
        }
    }
}

/**
 * @brief Очищает ресурсы разделяемой памяти.
 * 
 * @param shm_ptr Указатель на SHM.
 * @param shm_fd Файловый дескриптор.
 */
void cleanup(void* shm_ptr, int shm_fd) {
    munmap(shm_ptr, SHM_SIZE);
    close(shm_fd);
    shm_unlink(SHM_NAME); // Consumer является последним, кто использует память, поэтому он ее удаляет.
}


/**
 * @brief Основная функция "Consumer".
 *
 * @param outputPath Путь к выходному файлу.
 * @param use_compression Использовать ли декомпрессию.
 */
void consumer(const char* outputPath, bool use_compression) {
    std::ofstream outputFile(outputPath, std::ios::binary);
    if (!outputFile) {
        throw std::runtime_error("Failed to open output file");
    }

    int shm_fd = -1;
    SharedLayout* shared_layout = connect_to_shm(shm_fd);
    RingBuffer ringBuffer(shared_layout);
    uint64_t total_source_bytes = shared_layout->total_source_bytes.load();
    uint64_t total_bytes_written = 0;

    // Запуск потоков для декомпрессии и записи.
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < num_threads; ++i) {
        workers.emplace_back(decompression_worker, use_compression);
    }
    std::thread writer(writer_thread, std::ref(outputFile), std::ref(total_bytes_written));

    // Основной цикл чтения и сборки.
    enqueue_compressed_blocks(ringBuffer);

    // Сигнализируем воркерам, что новых данных не будет.
    readingComplete = true;
    workCv.notify_all();
    for (auto& worker : workers) {
        worker.join();
    }

    // Сигнализируем потоку записи о завершении.
    writer_should_finish = true;
    final_cv.notify_one();
    writer.join();

    cleanup(shared_layout, shm_fd);

    // --- Вывод метрик ---
    std::cout << "\n--- Consumer Metrics ---" << std::endl;
    std::cout << "Mode: " << (use_compression ? "COMPRESSED" : "UNCOMPRESSED") << std::endl;
    std::cout << "Source data size (from producer): " << total_source_bytes << " bytes" << std::endl;
    std::cout << "Total bytes written to file: " << total_bytes_written << " bytes" << std::endl;

    if (total_source_bytes > 0) {
        double loss_pct = (1.0 - static_cast<double>(total_bytes_written) / total_source_bytes) * 100.0;
        std::cout << "Data Loss: " << std::fixed << std::setprecision(4) << loss_pct << "%" << std::endl;
    }
}

// --- Точка входа ---
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <outputfile> [--nocompress]\n";
        return 1;
    }

    bool use_compression = true;
    if (argc > 2 && std::string(argv[2]) == "--nocompress") {
        use_compression = false;
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    try {
        consumer(argv[1], use_compression);
    } catch (const std::exception& e) {
        std::cerr << "Error in consumer: " << e.what() << '\n';
        shm_unlink(SHM_NAME);
        return 1;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "Consumer total time: " << duration.count() << " ms\n";

    return 0;
}
