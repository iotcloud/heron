#include <rdma/fi_eq.h>
#include <map>
#include <glog/logging.h>

#include "network/rdma/rdma_event_loop.h"

static void* loopEventsThreadNonBlock(void *param) {
  RDMAEventLoop* server = static_cast<RDMAEventLoop *>(param);
  server->Loop();
  return NULL;
}

RDMAEventLoop::RDMAEventLoop(RDMAFabric* _rdmaFabric) {
  int ret;
  this->fabric = _rdmaFabric->GetFabric();
  this->rdmaFabric = _rdmaFabric;
  this->run = true;
  this->fid_list = NULL;
  this->events = NULL;
  epfd = epoll_create1(0);
  if (epfd < 0) {
    ret = -errno;
    HPS_ERR("epoll_create1", ret);
    throw ret;
  }
}

void RDMAEventLoop::Loop() {
  int ret;

  while (run) {
    int size = (int) fids.size();

    memset(events, 0, sizeof events);
    LOG(INFO) << "Wait.......... wit size " << size;
    int trywait = fi_trywait(fabric, fid_list, size);
    if (trywait == FI_SUCCESS) {
      LOG(INFO) << "Wait success";
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd, events, size, -1));
      if (ret < 0) {
        ret = -errno;
        LOG(ERROR) << "epoll_wait " << ret;
      }
      for (int j = 0; j < ret; j++) {
        struct epoll_event *event = events + j;
        struct rdma_loop_info *cb = (struct rdma_loop_info *) event->data.ptr;
        if (cb != NULL) {
          cb->callback(AVAILABLE);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    } else if (trywait == -FI_EAGAIN){
      for (std::list<struct rdma_loop_info *>::iterator it=connections.begin();
                                             it!=connections.end(); ++it) {
        struct rdma_loop_info *c = *it;
        c->callback(TRYAGAIN);
      }
    }
  }
}

int RDMAEventLoop::RegisterRead(struct rdma_loop_info *connection) {
  struct epoll_event event;
  int ret;
  int fid = connection->fid;
  LOG(INFO) << "Register FID: " << fid;
  this->fids.push_back(connection->desc);
  this->connections.push_back(connection);

  int size = (int) fids.size();
  // get all the elements in fids and create a list
  if (fid_list) {
    delete[] fid_list;
  }
  fid_list = new struct fid*[size];
  int i = 0;
  for (std::list<struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
    fid_list[i] = *it;
    i++;
  }

  if (events) {
    delete[] events;
  }
  events = new struct epoll_event [size];

  event.data.ptr = (void *)connection;
  event.events = EPOLLIN;
  ret = epoll_ctl(epfd, EPOLL_CTL_ADD, fid, &event);
  if (ret) {
    ret = -errno;
    LOG(ERROR) << "epoll_ctl " << ret;
    return ret;
  }
  return 0;
}

int RDMAEventLoop::UnRegister(struct rdma_loop_info *loop) {
  struct epoll_event event;
  int ret;
  ret = epoll_ctl(epfd, EPOLL_CTL_DEL, loop->fid, &event);
  if (ret) {
    ret = -errno;
    LOG(ERROR) << "Failed to un-register connection " << ret;
    return ret;
  }
  return 0;
}

int RDMAEventLoop::Start() {
  int ret;
  // start the loop thread
  // ret = pthread_create(&loopThreadId, NULL, &loopEventsThreadNonBlock, (void *)this);
//  if (ret) {
//    LOG(ERROR) << "Failed to create thread " << ret;
//    return ret;
//  }

  return 0;
}

int RDMAEventLoop::Close() {
  this->run = false;
  return Wait();
}

int RDMAEventLoop::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

sp_int64 RDMAEventLoop::registerTimer(VCallback<RDMAEventLoop::Status> cb,
                                            bool persistent, sp_int64 mSecs) {
  return 0;
}


