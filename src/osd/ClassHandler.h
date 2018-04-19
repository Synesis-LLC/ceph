// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLASSHANDLER_H
#define CEPH_CLASSHANDLER_H

#include "include/types.h"
#include "objclass/objclass.h"
#include "common/Mutex.h"

#include <condition_variable>
#include <memory>

//forward declaration
class CephContext;
class OSD;

class ClassHandler
{
public:
  CephContext *cct;

  struct ClassData;

  struct ClassMethod {
    struct ClassHandler::ClassData *cls;
    string name;
    int flags;
    cls_method_call_t func;
    cls_method_cxx_call_t cxx_func;

    int exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata);
    void unregister();

    int get_flags() {
      Mutex::Locker l(cls->handler->mutex);
      return flags;
    }

    ClassMethod() : cls(0), flags(0), func(0), cxx_func(0) {}
  };

  struct ClassFilter {
    struct ClassHandler::ClassData *cls;
    std::string name;
    cls_cxx_filter_factory_t fn;

    void unregister();

    ClassFilter() : fn(0)
    {}
  };

  struct ClassData {
    enum Status {
      CLASS_UNKNOWN,
      CLASS_MISSING,         // missing
      CLASS_MISSING_DEPS,    // missing dependencies
      CLASS_INITIALIZING,    // calling init() right now
      CLASS_OPEN,            // initialized, usable
    } status;

    static std::string status_to_string(Status status)
    {
      switch(status){
      case CLASS_UNKNOWN: return "CLASS_UNKNOWN";
      case CLASS_MISSING: return "CLASS_MISSING";
      case CLASS_MISSING_DEPS: return "CLASS_MISSING_DEPS";
      case CLASS_INITIALIZING: return "CLASS_INITIALIZING";
      case CLASS_OPEN: return "CLASS_OPEN";
      default:
        return "unknown";
      };
    }

    string name;
    ClassHandler *handler;
    void *handle;

    bool whitelisted;

    map<string, ClassMethod> methods_map;
    map<string, ClassFilter> filters_map;

    set<ClassData *> dependencies;         /* our dependencies */
    set<ClassData *> missing_dependencies; /* only missing dependencies */

    ClassMethod *_get_method(const char *mname);

    ClassData() : status(CLASS_UNKNOWN),
                  handler(NULL),
                  handle(NULL) {}
    ~ClassData() { }

    ClassMethod *register_method(const char *mname, int flags, cls_method_call_t func);
    ClassMethod *register_cxx_method(const char *mname, int flags, cls_method_cxx_call_t func);
    void unregister_method(ClassMethod *method);

    ClassFilter *register_cxx_filter(
        const std::string &filter_name,
        cls_cxx_filter_factory_t fn);
    void unregister_filter(ClassFilter *method);

    ClassMethod *get_method(const char *mname) {
      Mutex::Locker l(handler->mutex);
      return _get_method(mname);
    }
    int get_method_flags(const char *mname);

    ClassFilter *get_filter(const std::string &filter_name)
    {
      Mutex::Locker l(handler->mutex);
      std::map<std::string, ClassFilter>::iterator i = filters_map.find(filter_name);
      if (i == filters_map.end()) {
        return NULL;
      } else {
        return &(i->second);
      }
    }
  };

  //TODO: move guard into ClassData, make ClassDataPtr template pointer for any class resource: filter, method, class
  struct ClassDataGuard
  {
    ClassDataGuard() :
      refcount(0),
      blocked(false)
    {}

    bool open_class_waits;

    std::atomic<uint32_t> refcount;
    std::atomic_bool blocked;

    void block()
    {
      blocked = true;
    }
    bool is_blocked()
    {
      return blocked;
    }
    void unblock()
    {
      blocked = false;
      open_class_cv.notify_all();
    }
    void incref()
    {
      refcount++;
    }
    bool is_used()
    {
      return refcount > 0;
    }
    void decref()
    {
      refcount--;
      if (refcount == 0) {
        close_class_cv.notify_all();
      }
    }

    std::mutex open_class_mutex;
    std::condition_variable open_class_cv;
    // timeout -> return false
    bool open_class_wait(int timeout_s)
    {
      if (timeout_s > 0) {
        if (this->is_blocked()) {
          std::unique_lock<std::mutex>lock(open_class_mutex);
          return open_class_cv.wait_for(lock, std::chrono::seconds(timeout_s),
                                        [this]()->bool{return !this->is_blocked();});
        } else {
          return true;
        }
      } else if (timeout_s < 0) {
        if (this->is_blocked()) {
          std::unique_lock<std::mutex>lock(open_class_mutex);
          open_class_cv.wait(lock, [this]()->bool{return !this->is_blocked();});
        }
        return true;
      } else {
        return false;
      }
    }

    bool open_class_maybe_wait(int timeout_s)
    {
      // first incref then check blocked
      incref();
      if (!is_blocked()) {
        return true;
      }
      if (!open_class_waits) {
        decref();
        return false;
      }
      if (timeout_s == 0) {
        decref();
        return false;
      }
      if (open_class_wait(timeout_s)) {
        return true;
      } else {
        decref();
        return false;
      }
    }

    std::mutex close_class_mutex;
    std::condition_variable close_class_cv;
    // timeout -> return false
    bool close_class_wait(int timeout_s)
    {
      if (timeout_s > 0) {
        if (this->is_used()) {
          std::unique_lock<std::mutex>lock(close_class_mutex);
          return close_class_cv.wait_for(lock, std::chrono::seconds(timeout_s),
                                         [this]()->bool{return !this->is_used();});
        } else {
          return true;
        }
      } else if (timeout_s < 0) {
        if (this->is_used()) {
          std::unique_lock<std::mutex>lock(close_class_mutex);
          close_class_cv.wait(lock, [this]()->bool{return !this->is_used();});
        }
        return true;
      } else {
        return false;
      }
    }
  };

  class ClassDataPtr
  {
  private:
    ClassDataGuard* cdg;
    ClassData* cls;
  public:
    ClassDataPtr() :
      cdg(nullptr),
      cls(nullptr)
    {}
    ClassDataPtr(const ClassDataPtr& src) :
      cdg(src.cdg),
      cls(src.cls)
    {
      if (cdg) {
        cdg->incref();
      }
    }
    ClassDataPtr(ClassDataPtr&& src) :
      cdg(src.cdg),
      cls(src.cls)
    {
      src.cdg = nullptr;
      src.cls = nullptr;
    }
    ClassDataPtr& operator=(const ClassDataPtr& src)
    {
      cdg = src.cdg;
      cls = src.cls;
      if (cdg) {
        cdg->incref();
      }
      return *this;
    }
    ClassDataPtr& operator=(ClassDataPtr&& src)
    {
      cdg = src.cdg;
      cls = src.cls;
      src.cdg = nullptr;
      src.cls = nullptr;
      return *this;
    }
    virtual ~ClassDataPtr()
    {
      if (cdg) {
        cdg->decref();
      }
    }
    ClassDataPtr(ClassDataGuard* _cdg, ClassData* _cls) : cdg(_cdg), cls(_cls) {}
    ClassData* operator->()
    {
      if (!cls) {
        // ClassDataPtr is not owner - like weak_ptr
        throw std::bad_weak_ptr();
      }
      return cls;
    }
    operator bool ()
    {
      return cls != nullptr && cdg != nullptr;
    }
  };

private:
  Mutex mutex;
  map<string, ClassData> classes;

  std::mutex class_guards_mutex;
  map<string, ClassDataGuard> class_guards;

  ClassData *_get_class(const string& cname, bool check_allowed);
  int _load_class(ClassData *cls);

  ClassDataGuard* _get_class_guard(const string& cname)
  {
    std::lock_guard<std::mutex>lock(class_guards_mutex);
    auto it = class_guards.find(cname);
    if (it == class_guards.end()) {
      return nullptr;
    }
    return &it->second;
  }

  static bool in_class_list(const std::string& cname,
      const std::string& list);

  int _open_class(const string& cname, ClassData **pcls);

  // add class name to blocked list -> prevents ongoing open_class calls
  // waits osd_close_class_timeout while all ClassData users be finished
  // then unloads class shared lib
  //
  // if timeout -> unblock and return error
  // if disable==true -> leave class in blocked list, else -> remove from blocked list after unload
  // if block_opens==true -> open_class waits with timeout, else -> open_class fail with error
  int close_class(const string& cname, bool disable, bool block_opens);

  int _unload_class(ClassData *cls);

public:
  explicit ClassHandler(CephContext *cct_) : cct(cct_), mutex("ClassHandler") {}

  int open_all_classes();

  void add_embedded_class(const string& cname);
  int open_class(const string& cname, ClassDataPtr* pcls);

  int list_classes(std::list<std::pair<std::string, std::string>>& class_list);

  // allow open_class
  int unblock_class(const string& cname)
  {
    ClassDataGuard* cdg = _get_class_guard(cname);
    if (cdg == nullptr) {
      return -ENOENT;
    }
    cdg->unblock();
    return 0;
  }

  int reload_class(const string& cname)
  {
    // open_class wait for class reload
    // class name will be unblocked after unload
    // and it will be loaded on-demand by next open_class call
    return close_class(cname, false, true);
  }

  int unload_and_block_class(const string& cname)
  {
    return close_class(cname, true, false);
  }

  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

  void shutdown();

  friend void cephd_preload_rados_classes(OSD *osd);
};


#endif
