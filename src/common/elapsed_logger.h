#ifndef LOG_GUARD_H
#define LOG_GUARD_H

#include <chrono>
#include <string>
#include <sstream>
#include <functional>

class elapsed_logger
{
public:
  thread_local static int level;
  std::chrono::high_resolution_clock::time_point started;
  std::function<void(const std::string&)> log_callback;
  std::string msg;

  elapsed_logger(const char* msg, std::function<void(const std::string&)> log_callback);
  virtual ~elapsed_logger();
};

#endif // LOG_GUARD_H
