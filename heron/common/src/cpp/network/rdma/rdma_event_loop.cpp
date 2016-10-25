#include <rdma/fi_eq.h>
#include <map>
#include <glog/logging.h>

#include "rdma_event_loop.h"

static void* loopEventsThreadNonBlock(void *param) {
  RDMAEventLoopNoneFD* server = static_cast<RDMAEventLoopNoneFD *>(param);
  server->Loop();
  return NULL;
}

RDMAEventLoopNoneFD::RDMAEventLoopNoneFD(struct fid_fabric *fabric) {
  this->run = true;
}

int RDMAEventLoopNoneFD::RegisterRead(struct rdma_loop_info *connection) {
  this->connections.push_back(connection);
  return 0;
}

void RDMAEventLoopNoneFD::Loop() {
  while (run) {
    for (std::list<struct rdma_loop_info *>::iterator it = connections.begin();
         it != connections.end(); ++it) {
      struct rdma_loop_info *c = *it;
      // LOG(INFO) << "Calling";
      c->callback(AVAILABLE);
    }
  }
  LOG(INFO) << "Loop exit";
}

int RDMAEventLoopNoneFD::Start() {
  int ret;
  // start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &loopEventsThreadNonBlock, (void *)this);
  if (ret) {
    LOG(ERROR) << "Failed to create thread " << ret;
    return ret;
  }

  return 0;
}

int RDMAEventLoopNoneFD::Close() {
  this->run = false;
  return Wait();
}

int RDMAEventLoopNoneFD::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

int RDMAEventLoopNoneFD::UnRegister(struct rdma_loop_info *con) {
  std::list<struct rdma_loop_info*>::iterator lpIt = connections.begin();
  while (lpIt != connections.end()) {
    struct rdma_loop_info* temp = *lpIt;
    if (temp == con) {
      connections.erase(lpIt);
      break;
    } else {
      lpIt++;
    }
  }
  return 0;
}

sp_int64 RDMAEventLoopNoneFD::registerTimer(VCallback<RDMAEventLoopNoneFD::Status> cb,
                                            bool persistent, sp_int64 mSecs) {
  return 0;
}



