#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

constexpr size_t SHM_SIZE = 256;
constexpr const char* SHM_NAME = "/shm_ring_buffer";
constexpr size_t RAW_BLOCK_FLOAT_COUNT = 64; // 256 байт

// Явная структура для общей памяти
struct SharedLayout {
    std::atomic<size_t> head;
    std::atomic<size_t> tail;
    // Данные для бенчмаркинга
    std::atomic<uint64_t> total_source_bytes;
    // Гибкий массив для данных
    uint8_t data_buffer[];
};

class RingBuffer {
public:
    RingBuffer(SharedLayout* layout);
    void write(const uint8_t* data, size_t length);
    void read(uint8_t* dest, size_t length);
    size_t getCapacity() const { return capacity_; }

private:
    SharedLayout* layout_;
    const size_t capacity_;
    size_t getFreeSpace() const;
    size_t getUsedSpace() const;
};

// --- Глобальные переменные для очередей и синхронизации ---

struct WorkItem {
    int index;
    std::vector<uint8_t> data;
};

extern std::queue<WorkItem> workQueue;
extern std::mutex workMutex;
extern std::condition_variable workCv;

struct ResultItem {
    int index;
    std::vector<uint8_t> data;
    size_t original_size;
};

extern std::queue<ResultItem> resultQueue;
extern std::mutex resultMutex;
extern std::condition_variable resultCv;

extern std::atomic<bool> readingComplete;

#endif // RING_BUFFER_H
