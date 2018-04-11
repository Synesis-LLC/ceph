// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 *
 *
 *
 */

#include <algorithm>
#include <string>
#include <sstream>
#include <errno.h>
#include <cmath>
#include <numeric>
#include <functional>
#include <memory>
#include <cstdint>

#include "objclass/objclass.h"

CLS_VER(2,0)
CLS_NAME(vector)

struct record_format
{
  enum VECTOR_ELEMENT_FORMAT : uint8_t
  {
    // explicit values used for serializing
    i8 = 1,
    i16 = 2,
    i32 = 3,
    i64 = 4,
    u8 = 5,
    u16 = 6,
    u32 = 7,
    u64 = 8,
    f32 = 9,
    f64 = 10
  } vformat;

  inline size_t get_element_size() const
  {
    switch (vformat) {
    case i8: return sizeof(int8_t);
    case i16: return sizeof(int8_t);
    case i32: return sizeof(int8_t);
    case i64: return sizeof(int8_t);
    case u8: return sizeof(uint8_t);
    case u16: return sizeof(uint16_t);
    case u32: return sizeof(uint32_t);
    case u64: return sizeof(uint64_t);
    case f32: return sizeof(float);
    case f64: return sizeof(double);
    }
    return 1;
  }

  inline const char* ve_format_to_str() const
  {
    switch (vformat) {
    case i8: return "i8";
    case i16: return "i16";
    case i32: return "i32";
    case i64: return "i64";
    case u8: return "u8";
    case u16: return "u16";
    case u32: return "u32";
    case u64: return "u64";
    case f32: return "f32";
    case f64: return "f64";
    }
    return "-";
  }

  static inline VECTOR_ELEMENT_FORMAT ve_format_from_str(char* ptr, char** pptr)
  {
    if (ptr[0] == 'f') {
      if (ptr[1] == '3' && ptr[2] == '2') {
        *pptr = ptr + 3;
        return f32;
      } else if (ptr[1] == '6' && ptr[2] == '4') {
        *pptr = ptr + 3;
        return f64;
      }
    }
    if (ptr[0] == 'u') {
      if (ptr[1] == '8') {
        *pptr = ptr + 2;
        return u8;
      } else if (ptr[1] == '1' && ptr[2] == '6') {
        *pptr = ptr + 3;
        return u16;
      } else if (ptr[1] == '3' && ptr[2] == '2') {
        *pptr = ptr + 3;
        return u32;
      } else if (ptr[1] == '6' && ptr[2] == '4') {
        *pptr = ptr + 3;
        return u64;
      }
    }
    if (ptr[0] == 'i') {
      if (ptr[1] == '8') {
        *pptr = ptr + 2;
        return i8;
      } else if (ptr[1] == '1' && ptr[2] == '6') {
        *pptr = ptr + 3;
        return i16;
      } else if (ptr[1] == '3' && ptr[2] == '2') {
        *pptr = ptr + 3;
        return i32;
      } else if (ptr[1] == '6' && ptr[2] == '4') {
        *pptr = ptr + 3;
        return i64;
      }
    }

    std::stringstream ss;
    ss << "invalid format: " << ptr;
    throw std::length_error(ss.str());
  }

  uint32_t vector_length;

  inline size_t get_vector_size() const
  {
    return vector_length*get_element_size();
  }

  uint32_t size;
  uint32_t vector_offset;

  static record_format from_string(const std::string& s)
  {
    record_format r;

    char* ptr = nullptr;
    r.size = std::strtol(s.c_str(), &ptr, 0);

    if (*ptr == '+') {
      ptr++;
      r.vector_offset = std::strtol(ptr, &ptr, 0);

      if (*ptr == ':') {
        ptr++;
        r.vformat = ve_format_from_str(ptr, &ptr);

        if (*ptr == 'x') {
          ptr++;
          r.vector_length = std::strtol(ptr, &ptr, 0);

          // allow formats like 0+0:i32x128
          if ((r.size == 0 && r.vector_offset == 0)
              || r.get_vector_size() + r.vector_offset <= r.size) {
            return r;
          }
        }
      }
    }
    std::stringstream ss;
    ss << "invalid format: " << s;
    throw std::length_error(ss.str());
  }
  /*
   * uint32 - record size
   * uint32 - vector offset
   * uint8  - element type
   * 3 x uint8 - reserved
   * uint32 - vector length
   */
  static record_format from_bl(const bufferlist& bl, size_t offset)
  {
    if (bl.length() < offset + 16) {
      std::stringstream ss;
      ss << "failed parse record_format: length=" << bl.length() << ", offset=" << offset;
      throw std::length_error(ss.str());
    }
    uint32_t buff[4];
    bl.copy(offset, sizeof(buff), (char*)&buff);
    return record_format(buff[0], buff[1], (VECTOR_ELEMENT_FORMAT)(buff[2] & 0x00ff), buff[3]);
  }

  void copy_to(bufferlist& bl) const
  {
    CLS_LOG(20, "record_format::copy_to bl %d", bl.length());
    bl.append((const char*)&size, 4);
    bl.append((const char*)&vector_offset, 4);
    bl.append((const char*)&vformat, 1);
    bl.append_zero(3);
    bl.append((const char*)&vector_length, 4);
  }

  record_format(size_t _size, size_t _offset, VECTOR_ELEMENT_FORMAT fmt, size_t len) :
    vformat(fmt),
    vector_length(len),
    size(_size),
    vector_offset(_offset)
  {}

  record_format() = default;

  std::string to_string() const
  {
    char buff[64];
    snprintf(buff, sizeof(buff), "%u+%u:%sx%u", size, vector_offset, ve_format_to_str(), vector_length);
    return std::string(buff);
  }
};

class base_record
{
  record_format format;
  std::vector<uint8_t> data;

  void check(const base_record& r) const
  {
    if (format.vector_length != r.format.vector_length) {
      std::stringstream ss;
      ss << "different dimentions: " << format.vector_length << " != " << r.format.vector_length;
      throw std::length_error(ss.str());
    }
    if (format.vformat != r.format.vformat) {
      std::stringstream ss;
      ss << "different vector format: " << format.to_string() << " != " << r.format.to_string();
      throw std::length_error(ss.str());
    }
  }

protected:
  virtual double _distance(const base_record& r) const = 0;

  base_record(const record_format& fmt) :
    format(fmt),
    data(fmt.size)
  {}
  virtual ~base_record() = default;

public:
  std::string to_string() const
  {
    char buff[16];
    snprintf(buff, sizeof(buff), "%ld", data.size());
    return format.to_string() + ", " + std::string(buff);
  }

  template<typename T>
  T* begin() const {
    return (T*)(data.data() + format.vector_offset);
  }

  template<typename T>
  T* end() const {
    return begin<T>() + format.vector_length;
  }

  static std::shared_ptr<base_record> read_from(const bufferlist& bl, size_t offset, const record_format& format);

  double distance(const base_record& r) const
  {
    check(r);
    return _distance(r);
  }

  void copy_to(bufferlist& bl) const {
    CLS_LOG(20, "base_record::copy_to bl %d", bl.length());
    CLS_LOG(20, "base_record::copy_to %s", to_string().c_str());
    int count = 0;
    for (uint8_t b : data) {
      CLS_LOG(30, "%04d %02x", count++, b);
    }
    bl.append((char*)data.data(), data.size());
  }

  void copy_format_to(bufferlist& bl) const {
    format.copy_to(bl);
  }

  size_t size() const
  {
    return format.size;
  }
};

template <typename T>
class record : public base_record
{
protected:
  virtual double _distance(const base_record& r) const override
  {
    return std::sqrt(
             std::inner_product(begin<T>(), end<T>(), r.begin<T>(), (double)0,
               std::plus<double>(),
               [] (T x1, T x2) -> double {
                 double tmp = (double)x1 - (double)x2;
                 return tmp*tmp;
               }));
  }

public:
  record<T>(const record_format& fmt) : base_record(fmt) {}
};

std::shared_ptr<base_record> base_record::read_from(const bufferlist& bl, size_t offset, const record_format& format)
{
  if (bl.length() < offset + format.size) {
    return nullptr;
  }
  std::shared_ptr<base_record> r;
  switch (format.vformat) {
  case record_format::i8:  r = std::make_shared<record<int8_t>>(format); break;
  case record_format::i16: r = std::make_shared<record<int16_t>>(format); break;
  case record_format::i32: r = std::make_shared<record<int32_t>>(format); break;
  case record_format::i64: r = std::make_shared<record<int64_t>>(format); break;
  case record_format::u8:  r = std::make_shared<record<uint8_t>>(format); break;
  case record_format::u16: r = std::make_shared<record<uint16_t>>(format); break;
  case record_format::u32: r = std::make_shared<record<uint32_t>>(format); break;
  case record_format::u64: r = std::make_shared<record<uint64_t>>(format); break;
  case record_format::f32:   r = std::make_shared<record<float>>(format); break;
  case record_format::f64:   r = std::make_shared<record<double>>(format); break;
  }
  bl.copy(offset, r->data.size(), (char*)r->data.data());
  return std::move(r);
}

struct request
{
  uint32_t records_to_find;
  std::shared_ptr<base_record> rec;

  static request from_bl(const bufferlist& bl, size_t offset)
  {
    if (bl.length() <= offset + 4 + 16) {
      std::stringstream ss;
      ss << "failed parse request: length=" << bl.length() << ", offset=" << offset;
      throw std::length_error(ss.str());
    }
    request r;
    bl.copy(offset, sizeof(r.records_to_find), (char*)&r.records_to_find);
    offset += 4;
    record_format fmt = record_format::from_bl(bl, offset);
    offset += 16;
    CLS_LOG(20, "request::from_bl %u %s", r.records_to_find, fmt.to_string().c_str());
    r.rec = base_record::read_from(bl, offset, fmt);
    if (!r.rec) {
      std::stringstream ss;
      ss << "failed parse request: length=" << bl.length() << ", offset=" << offset;
      throw std::length_error(ss.str());
    }
    return std::move(r);
  }
  void copy_to(bufferlist& bl) const
  {
    bl.append((const char*)&records_to_find, 4);
    rec->copy_to(bl);
  }
  size_t size() const
  {
    return 4 + 16 + rec->size();
  }
};

struct multi_request
{
  std::vector<request> requests;
  /*
   * Request format
   *
   * uint32 - number of parallel requests
   * each request:
   *   uint32 - number of records to find
   *   16 bytes - record_format
   *   record_format.size bytes - record data
   */
  static multi_request from_bl(const bufferlist& bl, size_t offset)
  {
    if (bl.length() <= offset + 4 + 4 + 16) {
      std::stringstream ss;
      ss << "failed parse request: length=" << bl.length() << ", offset=" << offset;
      throw std::length_error(ss.str());
    }
    multi_request mr;
    uint32_t n = 0;
    bl.copy(offset, sizeof(n), (char*)&n);
    CLS_LOG(20, "multi_request::from_bl %u", n);
    mr.requests.resize(n);
    offset += 4;
    for (size_t i = 0; i < n; i++) {
      mr.requests[i] = request::from_bl(bl, offset);
      offset += mr.requests[i].size();
    }
    return std::move(mr);
  }
};

struct response
{
  std::shared_ptr<base_record> rqst;
  std::list<std::pair<double, std::shared_ptr<base_record>>> results;

  uint32_t max_to_find;

  double max_distance;

  response(const request& rq) :
    rqst(rq.rec),
    max_to_find(rq.records_to_find),
    max_distance(0)
  {}

  void copy_to(bufferlist& bl) const
  {
    CLS_LOG(20, "response::copy_to bl %d", bl.length());
    CLS_LOG(20, "response::copy_to %s", rqst->to_string().c_str());
    rqst->copy_format_to(bl);
    rqst->copy_to(bl);
    uint32_t len = results.size();
    CLS_LOG(20, "response::copy_to %u", len);
    bl.append((const char*)&len, 4);
    for (const auto& res : results) {
      CLS_LOG(20, "response::copy_to %f", res.first);
      bl.append((const char*)&res.first, 8);
      CLS_LOG(20, "response::copy_to %s", res.second->to_string().c_str());
      res.second->copy_to(bl);
    }
  }

  void test_append(std::shared_ptr<base_record> r)
  {
    double d = -1;
    try {
      d = rqst->distance(*r);
    } catch (const std::exception& e) {
      CLS_LOG(0, "%s", e.what());
      return;
    }
    if (results.size() < max_to_find) {
      // window is not full
      results.emplace_back(d, r);
      if (max_distance < d) {
        max_distance = d;
      }
    } else if (max_distance == d) {
      // r is same, may exceed max number of results
      results.emplace_back(d, r);
    } else if (max_distance > d) {
      // r is closer
      results.emplace_back(d, r);
      double new_max_distance = 0;
      results.remove_if([&new_max_distance, this](const std::pair<double, std::shared_ptr<base_record>>& p) {
        if (p.first == max_distance) {
          return true;
        } else if (p.first > new_max_distance) {
          new_max_distance = p.first;
        }
        return false;
      });
      max_distance = new_max_distance;
    }
  }
};

struct multi_response
{
  std::vector<response> responses;
  record_format results_format;

  multi_response(const multi_request& mrq, const record_format& fmt) :
    results_format(fmt)
  {
    for (const auto& rq : mrq.requests) {
      responses.emplace_back(rq);
    }
  }

  /*
   * Response format
   *
   * 16 bytes - results records format
   * uint32 - number of responses
   * each response:
   *   16 bytes - request record format
   *   record_format.size bytes - request record data
   *   uint32 - number of result records
   *   each result record:
   *     float64 - distance
   *     record_format.size bytes - result record data
   */
  void copy_to(bufferlist& bl) const
  {
    CLS_LOG(20, "multi_response::copy_to bl %d", bl.length());
    results_format.copy_to(bl);
    uint32_t len = responses.size();
    CLS_LOG(20, "multi_response::copy_to %u", len);
    bl.append((const char*)&len, 4);
    for (const auto& res : responses) {
      res.copy_to(bl);
    }
  }
};

#define READ_BYTES (1024*1024)

static int find_closest(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // get lnegth of vectors in object from xattr "length"
  bufferlist attr_bl;
  int err = cls_cxx_getxattr(hctx, "format", &attr_bl);
  if (err < 0) {
    const char* errmsg = "error reading object \"format\" xattr";
    CLS_LOG(0, "%s", errmsg);
    out->append(errmsg);
    return err;
  }
  record_format rfmt;
  try {
    rfmt = record_format::from_string(attr_bl.to_str());
  } catch (const std::exception& e) {
    CLS_LOG(0, "%s", e.what());
    out->append(e.what());
    return -EINVAL;
  }
  CLS_LOG(20, "find_closest %s", rfmt.to_string().c_str());
  CLS_LOG(20, "find_closest %u", in->length());

  multi_request mrq;
  try {
    mrq = multi_request::from_bl(*in, 0);
  } catch (const std::exception& e) {
    CLS_LOG(0, "%s", e.what());
    out->append(e.what());
    return -EINVAL;
  }

  CLS_LOG(20, "%lu %u %s",
          mrq.requests.size(),
          mrq.requests[0].records_to_find,
          mrq.requests[0].rec->to_string().c_str());

  multi_response mrsp(mrq, rfmt);

  uint64_t obj_size = 0;
  time_t mtime;
  err = cls_cxx_stat(hctx, &obj_size, &mtime);  if (err < 0) {
    out->append("error get object stat");
    return err;
  }
  if (obj_size == 0) {
    // empty object - empty result (nothing found)
    mrsp.copy_to(*out);
    return 0;
  }

  uint32_t read_length = READ_BYTES / rfmt.size;
  read_length *= rfmt.size;
  uint64_t read_count = (obj_size / read_length) + 1;
  int obj_offset = 0;
  bufferlist data_bl;
  while (read_count--) {
    data_bl.clear();
    // read READ_BYTES into memory each time
    err = cls_cxx_read(hctx, obj_offset, read_length, &data_bl);
    if (err < 0) {
      out->append("read error");
      return -EIO;
    }
    if (data_bl.length() < rfmt.size) {
      break;
    }
    CLS_LOG(30, "cls_cxx_read %d %d", obj_offset, data_bl.length());
    obj_offset += data_bl.length();

    // foreach record in readed data compare disatance with min distance
    int count = data_bl.length() / rfmt.size;
    int data_bl_offset = 0;
    while (count--) {
      CLS_LOG(30, "base_record::read_from %d %u", data_bl_offset, rfmt.size);
      auto rec = base_record::read_from(data_bl, data_bl_offset, rfmt);
      if (!rec) {
        CLS_LOG(0, "read next record failed %d %lu %d %d", obj_offset, read_count, data_bl_offset, count);
        continue;
      }
      CLS_LOG(30, "%s", rec->to_string().c_str());
      data_bl_offset += rfmt.size;
      for (auto& resp : mrsp.responses) {
        resp.test_append(rec);
      }
    }
  }

  mrsp.copy_to(*out);
  return 0;
}


CLS_INIT(vector)
{
  CLS_LOG(0, "loading cls_vector");

  cls_handle_t h_class;
  cls_method_handle_t h_find_closest;

  cls_register("vector", &h_class);

  cls_register_cxx_method(h_class, "find_closest", CLS_METHOD_RD | CLS_METHOD_PROMOTE, find_closest, &h_find_closest);
}
