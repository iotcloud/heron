/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "util/xor-manager.h"
#include <iostream>
#include <map>
#include <vector>
#include <unordered_map>
#include "util/rotating-map.h"
#include "config/heron-internals-config-reader.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

XorManager::XorManager(EventLoop* eventLoop, sp_int32 _timeout,
                       const std::vector<sp_int32>& _task_ids)
    : eventLoop_(eventLoop), timeout_(_timeout) {
  n_buckets_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrXormgrRotatingmapNbuckets();

//  eventLoop_->registerTimer([this](EventLoop::Status status) { this->rotate(status); }, false,
//                            _timeout * 1000000);
  std::vector<sp_int32>::const_iterator iter;
  for (iter = _task_ids.begin(); iter != _task_ids.end(); ++iter) {
    tasks_[*iter] = new RotatingMap(n_buckets_);
  }
  pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
}

XorManager::~XorManager() {
  std::map<sp_int32, RotatingMap*>::iterator iter;
  for (iter = tasks_.begin(); iter != tasks_.end(); ++iter) {
    delete iter->second;
  }
}

void XorManager::rotate(EventLoopImpl::Status) {
  pthread_spin_lock(&lock);
  std::map<sp_int32, RotatingMap*>::iterator iter;
  for (iter = tasks_.begin(); iter != tasks_.end(); ++iter) {
    iter->second->rotate();
  }
  sp_int32 timeout = timeout_ / n_buckets_ + timeout_ % n_buckets_;
  pthread_spin_unlock(&lock);
//  eventLoop_->registerTimer([this](EventLoop::Status status) { this->rotate(status); }, false,
//                            timeout * 1000000);
}

void XorManager::create(sp_int32 _task_id, sp_int64 _key, sp_int64 _value) {
  pthread_spin_lock(&lock);
  CHECK(tasks_.find(_task_id) != tasks_.end());
  tasks_[_task_id]->create(_key, _value);
  pthread_spin_unlock(&lock);
}

bool XorManager::anchor(sp_int32 _task_id, sp_int64 _key, sp_int64 _value) {
  pthread_spin_lock(&lock);
  CHECK(tasks_.find(_task_id) != tasks_.end());
  bool ret = tasks_[_task_id]->anchor(_key, _value);
  pthread_spin_unlock(&lock);
  return ret;
}

bool XorManager::remove(sp_int32 _task_id, sp_int64 _key) {
  pthread_spin_lock(&lock);
  CHECK(tasks_.find(_task_id) != tasks_.end());
  bool ret = tasks_[_task_id]->remove(_key);
  pthread_spin_unlock(&lock);
  return ret;
}

}  // namespace stmgr
}  // namespace heron
