#include <rdma/fi_eq.h>
#include <map>
#include <glog/logging.h>

#include "network/rdma/rdma_event_loop.h"

// the initial size of the event array and fid array
const sp_int32 __DEFAULT_EVENT_CAPACITY__ = 1024;

static void *loopEventsThreadNonBlock(void *param) {
  RDMAEventLoop *loop = static_cast<RDMAEventLoop *>(param);
  loop->Loop();
  return NULL;
}

RDMAEventLoop::RDMAEventLoop(RDMAFabric *_rdmaFabric) {
  int ret;
  this->fabric = _rdmaFabric->GetFabric();
  this->rdmaFabric = _rdmaFabric;
  this->run_ = true;
  this->current_capacity_ = __DEFAULT_EVENT_CAPACITY__;
  this->events_ = new struct epoll_event[current_capacity_];
  this->fids_ = new fid_t[current_capacity_];
  this->to_unregister_items = 0;
  // create the epoll
  epfd_ = epoll_create1(0);
  if (epfd_ < 0) {
    ret = -errno;
    LOG(FATAL) << "Epoll creation failed: " << strerror(errno);
    throw ret;
  }
  pthread_spin_init(&spinlock_, PTHREAD_PROCESS_PRIVATE);
}

void RDMAEventLoop::Loop() {
  int ret;
  while (run_) {
    int size = (int) event_details.size();
    // nothing to listen on
    if (size == 0) {
      sleep(1);
      continue;
    }

    memset(events_, 0, sizeof (struct epoll_event) * current_capacity_);
    pthread_spin_lock(&spinlock_);
    int trywait = fi_trywait(fabric, fids_, size);
    pthread_spin_unlock(&spinlock_);
    if (trywait == FI_SUCCESS) {
      errno = 0;
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd_, events_, size, -1));
      if (ret < 0) {
        ret = -errno;
        LOG(WARNING) << "Error in epoll wait " << ret;
      }
      for (int j = 0; j < ret; j++) {
        struct epoll_event *event = events_ + j;
        struct rdma_loop_info *cb = (struct rdma_loop_info *) event->data.ptr;
        if (cb != NULL) {
//          LOG(INFO) << "calling callback with FID:" << cb->fid << " ";
          cb->callback(AVAILABLE);
        } else {
          LOG(ERROR) << "Connection NULL";
        }
      }
    } else if (trywait == -FI_EAGAIN) {
//      LOG(INFO) << "Wait try again";
      // check weather current event details and event details are different, if so make them equal
      if (current_event_details.size() != event_details.size()) {
        current_event_details.clear();
        for (std::vector<struct rdma_loop_info *>::iterator it = event_details.begin();
             it != event_details.end(); ++it) {
          current_event_details.push_back(*it);
        }
      }

      for (std::vector<struct rdma_loop_info *>::iterator it = current_event_details.begin();
           it != current_event_details.end(); ++it) {
        struct rdma_loop_info *c = *it;
//        LOG(INFO) << "calling callback with FID:" << c->fid << " ";
        c->callback(TRYAGAIN);
      }
    }

//    if (to_unregister_items > 0) {
//      RemoveItems();
//    }
  }
}

int RDMAEventLoop::RegisterRead(struct rdma_loop_info *info) {
  struct epoll_event *event = new struct epoll_event();
  int ret;
  // add the vent to the list
  this->event_details.push_back(info);
  int size = (int) event_details.size();
  // get all the elements in fids and create a list
  if (size > current_capacity_) {
    sp_int32 new_capacity = this->current_capacity_ * 2;
    struct epoll_event* events = new struct epoll_event[new_capacity];
    struct fid **fids = new struct fid *[new_capacity];
    memcpy(fids, fids_, sizeof (struct fid *) * this->current_capacity_);
    memcpy(events, events_, sizeof (struct epoll_event) * this->current_capacity_);
    delete []events_;
    delete []fids_;
    events_ = events;
    fids_ = fids;
  }

  // add the fid so we can use trywait
  fids_[size - 1] = info->desc;

  event->data.ptr = (void *) info;
  event->events = EPOLLIN;
  ret = epoll_ctl(epfd_, EPOLL_CTL_ADD, info->fid, event);
  if (ret) {
    ret = -errno;
    LOG(FATAL) << "Failed to register event queue" << strerror(errno);
    return ret;
  }

  return 0;
}

int RDMAEventLoop::UnRegister(struct rdma_loop_info *info) {
  LOG(INFO) << "Un-register fid: " << info->fid;
  info->valid = false;
  to_unregister_items++;
  pthread_spin_lock(&spinlock_);
  for (std::vector<struct rdma_loop_info *>::iterator it = event_details.begin();
       it != event_details.end(); it++) {
    if (!(*it)->fid == info->fid) {
      LOG(INFO) << "Remove item with FID: " << (*it)->fid;
      it = event_details.erase(it);
    }
  }

  sp_int32 index = 0;
  memset(fids_, 0, sizeof (struct fid *) * this->current_capacity_);
  // construct the fid list again we don't downsize
  for (std::vector<struct rdma_loop_info *>::iterator it = event_details.begin();
       it != event_details.end(); it++) {
    fids_[index] = (*it)->desc;
    index++;
  }
  pthread_spin_unlock(&spinlock_);
  return 0;
}

int RDMAEventLoop::Start() {
  int ret;
  //start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &loopEventsThreadNonBlock, (void *) this);
  if (ret) {
    LOG(ERROR) << "Failed to create thread " << ret;
    return ret;
  }

  return 0;
}

int RDMAEventLoop::Stop() {
  this->run_ = false;
  close(epfd_);
  return 0;
}

int RDMAEventLoop::Wait() {
  pthread_join(loopThreadId, NULL);
  delete []events_;
  delete []fids_;
  return 0;
}

sp_int64 RDMAEventLoop::registerTimer(VCallback<RDMAEventLoop::Status> cb,
                                      bool persistent, sp_int64 mSecs) {
  return 0;
}


