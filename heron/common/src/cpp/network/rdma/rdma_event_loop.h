#ifndef HERON_RDMA_EVENT_LOOP_H_
#define HERON_RDMA_EVENT_LOOP_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <unordered_map>

#include <rdma/fabric.h>
#include <list>
#include <functional>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/hps.h"
#include "network/rdma/rdma_fabric.h"
#include "basics/basics.h"

enum rdma_loop_status {AVAILABLE, TRYAGAIN};

enum rdma_loop_event {
  CONNECTION,
  CQ_READ,
  CQ_TRANSMIT
};

// Represents a callback that returns void but takes any number of
// input arguments.
template <typename... Args>
using VCallback = std::function<void(Args...)>;

struct rdma_loop_info {
  VCallback<enum rdma_loop_status> callback;
  int fid;
  fid_t desc;
  enum rdma_loop_event event;
  bool valid;
};

class RDMAEventLoop {
public:
  enum Status {};
  RDMAEventLoop(RDMAFabric *rdmaFabric);
  int RegisterRead(struct rdma_loop_info *info);
  int UnRegister(struct rdma_loop_info *info);
  sp_int64 registerTimer(VCallback<RDMAEventLoop::Status> cb, bool persistent,
                         sp_int64 mSecs);
  RDMAFabric * get_fabric() { return rdmaFabric; }
  void Loop();
  int Start();
  int Stop();
  int Wait();
private:
  bool run_;
  pthread_t loopThreadId;
  RDMAFabric *rdmaFabric;
  struct fid_fabric *fabric;
  int epfd_;
  struct fid **fids_;
  struct epoll_event* events_;
  // current capacity of events and fids
  sp_int32 current_capacity_;
  std::vector<struct rdma_loop_info *> event_details;
  sp_int32 to_unregister_items;
  pthread_spinlock_t spinlock_;
};

#endif