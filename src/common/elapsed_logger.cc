#include "common/elapsed_logger.h"

thread_local int elapsed_logger::level = 0;

elapsed_logger::elapsed_logger(const char* msg, std::function<void(const std::string&)> log_callback) :
  started(std::chrono::high_resolution_clock::now()),
  log_callback(log_callback),
  msg(msg)
{
  std::stringstream ss;
  ss << level << " " << msg << " entered";
  log_callback(ss.str());
  level++;
}

elapsed_logger::~elapsed_logger()
{
  level--;
  std::stringstream ss;
  ss << level << " " << msg << " elapsed "
     << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - started).count()
     << " us";
  log_callback(ss.str());
}
