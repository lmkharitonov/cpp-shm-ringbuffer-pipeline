cmake_minimum_required(VERSION 3.10)
project(ProducerConsumerPipeline LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

include_directories(${PROJECT_SOURCE_DIR}/include)

# Возвращаем зависимость от zstd
add_executable(producer src/producer.cpp src/ring_buffer.cpp)
target_link_libraries(producer PRIVATE zstd pthread)

add_executable(consumer src/consumer.cpp src/ring_buffer.cpp)
target_link_libraries(consumer PRIVATE zstd pthread)

