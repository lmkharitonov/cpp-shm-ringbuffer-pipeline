#include "ring_buffer.h"
#include <cstring>
#include <stdexcept>
#include <thread>

// Определения глобальных переменных
std::queue<WorkItem> workQueue;
std::mutex workMutex;
std::condition_variable workCv;

std::queue<ResultItem> resultQueue;
std::mutex resultMutex;
std::condition_variable resultCv;

std::atomic<bool> readingComplete(false);

// Реализация методов RingBuffer
RingBuffer::RingBuffer(SharedLayout* layout)
    : layout_(layout),
      capacity_(SHM_SIZE - sizeof(SharedLayout)) {}

size_t RingBuffer::getUsedSpace() const {
    size_t head = layout_->head.load(std::memory_order_acquire);
    size_t tail = layout_->tail.load(std::memory_order_acquire);
    if (tail >= head) return tail - head;
    return capacity_ - head + tail;
}

size_t RingBuffer::getFreeSpace() const {
    return capacity_ - getUsedSpace() - 1;
}

void RingBuffer::write(const uint8_t* data, size_t length) {
    while (getFreeSpace() < length) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    size_t tail = layout_->tail.load(std::memory_order_relaxed);
    for (size_t i = 0; i < length; ++i) {
        layout_->data_buffer[(tail + i) % capacity_] = data[i];
    }
    layout_->tail.store((tail + length) % capacity_, std::memory_order_release);
}

void RingBuffer::read(uint8_t* dest, size_t length) {
    while (getUsedSpace() < length) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    size_t head = layout_->head.load(std::memory_order_relaxed);
    for (size_t i = 0; i < length; ++i) {
        dest[i] = layout_->data_buffer[(head + i) % capacity_];
    }
    layout_->head.store((head + length) % capacity_, std::memory_order_release);
}
