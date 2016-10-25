#ifndef EVENT_LOOP_H_
#define EVENT_LOOP_H_

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

#include "rdma_event_loop.h"
#include "hps.h"
#include "sptypes.h"

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
};

class RDMAEventLoopNoneFD {
public:
  enum Status {};
  RDMAEventLoopNoneFD(struct fid_fabric *fabric);
  int RegisterRead(struct rdma_loop_info *connection);
  int UnRegister(struct rdma_loop_info *con);
  sp_int64 registerTimer(VCallback<RDMAEventLoopNoneFD::Status> cb, bool persistent,
                                        sp_int64 mSecs);
  void Loop();
  int Start();
  int Close();
  int Wait();

private:
  bool run;
  pthread_t loopThreadId;
  std::list<struct fid *> fids;
  std::list<struct rdma_loop_info *> connections;
};

#endif