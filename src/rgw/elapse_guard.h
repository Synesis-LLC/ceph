//
// Created by aleksei on 5/25/17.
//

#ifndef CEPH_ELAPSE_GUARD_H
#define CEPH_ELAPSE_GUARD_H


class elapse_guard
{
 public:
  uint64_t start;
  const char* name;
  elapse_guard(const char* name) : name(name)
  {
    start = ceph_clock_now().to_nsec();
  }
  virtual ~elapse_guard()
  {
    uint64_t elapsed = ceph_clock_now().to_nsec() - start;
    dout(10) << "elapsed " << name << " " << elapsed/1000000 << dendl;
  }
};

#endif //CEPH_ELAPSE_GUARD_H
