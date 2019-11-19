#ifndef __STOP_WATCH_H__
#define __STOP_WATCH_H__

#include <chrono>
#include <string>
#include <iostream>

struct Stopwatch {
  using time_point =
      std::chrono::time_point<std::chrono::high_resolution_clock>;
  explicit Stopwatch(const std::string &message)
      : message(message) {}

  void Start() { start_time = std::chrono::high_resolution_clock::now(); }
  void End() {
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = end_time - start_time;
    std::cout << message << ", duration: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(duration)
                     .count()
              << " ms\n";
  }

  std::string message;
  time_point start_time;
};

#endif
