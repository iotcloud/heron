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

#include "manager/stmgr-clientmgr.h"
#include <iostream>
#include <set>
#include <map>
#include "manager/stmgr.h"
#include "manager/stmgr-client.h"
#include "manager/rdma/rdma_stmgr_client.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"
#include "metrics/metrics.h"
#include <netdb.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace heron {
namespace stmgr {

// New connections made with other stream managers.
const sp_string METRIC_STMGR_NEW_CONNECTIONS = "__stmgr_new_connections";

StMgrClientMgr::StMgrClientMgr(EventLoop* eventLoop, RDMAEventLoopNoneFD *rdmaEventLoop, const sp_string& _topology_name,
                               const sp_string& _topology_id, const sp_string& _stmgr_id,
                               StMgr* _stream_manager,
                               heron::common::MetricsMgrSt* _metrics_manager_client)
    : topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      eventLoop_(eventLoop),
      rdmaEventLoop_(rdmaEventLoop),
      stream_manager_(_stream_manager),
      metrics_manager_client_(_metrics_manager_client) {
  stmgr_clientmgr_metrics_ = new heron::common::MultiCountMetric();
  metrics_manager_client_->register_metric("__clientmgr", stmgr_clientmgr_metrics_);
}

StMgrClientMgr::~StMgrClientMgr() {
  // This should not be called
  metrics_manager_client_->unregister_metric("__clientmgr");
  delete stmgr_clientmgr_metrics_;
}

void StMgrClientMgr::NewPhysicalPlan(const proto::system::PhysicalPlan* _pplan) {
  // TODO(vikasr) : Currently we establish connections with all streammanagers
  // In the next iteration we might want to make it better
  std::set<sp_string> all_stmgrs;
  for (sp_int32 i = 0; i < _pplan->stmgrs_size(); ++i) {
    const proto::system::StMgr& s = _pplan->stmgrs(i);
    if (s.id() == stmgr_id_) {
      continue;  // dont want to connect to ourselves
    }
    all_stmgrs.insert(s.id());
    if (clients_.find(s.id()) != clients_.end()) {
      // We already have a connection for this stmgr.
      // Just make sure we have it for the same host/port
      const NetworkOptions& o = clients_[s.id()]->get_clientoptions();
      if (o.get_host() != s.host_name() || o.get_port() != s.data_port()) {
        LOG(INFO) << "Stmgr " << s.id() << " changed from " << o.get_host() << ":" << o.get_port()
                  << " to " << s.host_name() << ":" << s.data_port();
        // This stmgr has actually moved to a different host/port
        clients_[s.id()]->Quit();  // this will delete itself.
        rdma_clients_[s.id()]->Quit();
        clients_[s.id()] = CreateClient(s.id(), s.host_name(), s.data_port());
        rdma_clients_[s.id()] = CreateRDMAClient(s.id(), s.host_name(), 24499);
      } else {
        // This stmgr has remained the same. Don't do anything
      }
    } else {
      // We don't have any connection to this stmgr.
      LOG(INFO) << "Stmgr " << s.id() << " came on " << s.host_name() << ":" << s.data_port();
      clients_[s.id()] = CreateClient(s.id(), s.host_name(), s.data_port());
      rdma_clients_[s.id()] = CreateRDMAClient(s.id(), s.host_name(), 24499);
    }
  }

  // We need to remove any unused ports
  std::set<sp_string> to_remove;
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    if (all_stmgrs.find(iter->first) == all_stmgrs.end()) {
      // This stmgr is no longer there in the physical map
      to_remove.insert(iter->first);
    }
  }

  // Now go over to_remove to remove all the unused stmgrs
  for (auto iter = to_remove.begin(); iter != to_remove.end(); ++iter) {
    LOG(INFO) << "Stmgr " << *iter << " no longer required";
    clients_[*iter]->Quit();  // This will delete itself.
    clients_.erase(*iter);
  }
}

bool StMgrClientMgr::DidAnnounceBackPressure() {
  return stream_manager_->DidAnnounceBackPressure();
}

StMgrClient* StMgrClientMgr::CreateClient(const sp_string& _other_stmgr_id,
                                          const sp_string& _hostname, sp_int32 _port) {
  stmgr_clientmgr_metrics_->scope(METRIC_STMGR_NEW_CONNECTIONS)->incr();
  NetworkOptions options;
  options.set_host(_hostname);
  options.set_port(_port);
  options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                  ->GetHeronStreammgrNetworkOptionsMaximumPacketMb() *
                              1024 * 1024);
  options.set_socket_family(PF_INET);
  StMgrClient* client = new StMgrClient(eventLoop_, options, topology_name_, topology_id_,
                                        stmgr_id_, _other_stmgr_id, this, metrics_manager_client_);
  client->Start();
  return client;
}

RDMAStMgrClient* StMgrClientMgr::CreateRDMAClient(const sp_string& _other_stmgr_id,
                                          const sp_string& _hostname, sp_int32 _port) {
  stmgr_clientmgr_metrics_->scope(METRIC_STMGR_NEW_CONNECTIONS)->incr();
  char *port_str_ = new char[15];
  std::map<sp_string, sp_string> ips = {{"149.165.150.51", "192.168.0.101"},
  {"149.165.150.52", "192.168.0.102"},
  {"149.165.150.53", "192.168.0.103"}, {"149.165.150.54", "192.168.0.104"},
  {"149.165.150.55", "192.168.0.105"}, {"149.165.150.56", "192.168.0.106"},
  {"149.165.150.57", "192.168.0.107"}, {"149.165.150.58", "192.168.0.108"},
  {"149.165.150.60", "192.168.0.100"}, {"149.165.150.60", "149.165.150.52.168.0.110"},
  {"149.165.150.61", "192.168.0.111"}, {"149.165.150.62", "192.168.0.112"}};
  LOG(INFO) << "Connecting to original: "  << _hostname << ":" << _port;
  hostent * record = gethostbyname(_hostname.c_str());
  if(record == NULL) {
    LOG(ERROR) << _hostname << " is unavailable";
  }
  in_addr * address = (in_addr * )record->h_addr;
  string ip_address = inet_ntoa(* address);
  string ib_address = ips[ip_address];

  char * hostname = new char[ib_address.size() + 1];
  std::copy(ib_address.begin(), ib_address.end(), hostname);
  hostname[ib_address.size()] = '\0';

  RDMAOptions *options = new RDMAOptions();

  options->buf_size = 1024 * 640;
  options->no_buffers = 10;
  sprintf(port_str_, "%d", _port);
  options->SetDest(hostname, port_str_);
  LOG(INFO) << "Connecting to: "  << hostname << ":" << port_str_;

  RDMAFabric *fabric = new RDMAFabric(options);
  fabric->Init();

  RDMAStMgrClient* client = new RDMAStMgrClient(rdmaEventLoop_, eventLoop_,
                                        options, fabric, topology_name_, topology_id_,
                                        stmgr_id_, _other_stmgr_id, this);
  client->Start();
  return client;
}

void StMgrClientMgr::SendTupleStreamMessage(sp_int32 _task_id, const sp_string& _stmgr_id,
                                            const proto::system::HeronTupleSet2& _msg) {
  auto iter = clients_.find(_stmgr_id);
  auto rdmaIter = rdma_clients_.find(_stmgr_id);
//  CHECK(iter != clients_.end());

  // Acquire the message
  proto::stmgr::TupleStreamMessage2* out = clients_[_stmgr_id]->acquire(out);
  out->set_task_id(_task_id);
  _msg.SerializePartialToString(out->mutable_set());

  // give priority to rdma
  if (rdmaIter != rdma_clients_.end()) {
    rdma_clients_[_stmgr_id]->SendTupleStreamMessage(out);
    // Release the message
    // clients_[_stmgr_id]->release(out);
  } else {
    clients_[_stmgr_id]->SendTupleStreamMessage(*out);
    // Release the message
    clients_[_stmgr_id]->release(out);
  }
}

void StMgrClientMgr::StartBackPressureOnServer(const sp_string& _other_stmgr_id) {
  stream_manager_->StartBackPressureOnServer(_other_stmgr_id);
}

void StMgrClientMgr::StopBackPressureOnServer(const sp_string& _other_stmgr_id) {
  // Call the StMgrServers removeBackPressure method
  stream_manager_->StopBackPressureOnServer(_other_stmgr_id);
}

void StMgrClientMgr::SendStartBackPressureToOtherStMgrs() {
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    iter->second->SendStartBackPressureMessage();
  }
}

void StMgrClientMgr::SendStopBackPressureToOtherStMgrs() {
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    iter->second->SendStopBackPressureMessage();
  }
}

}  // namespace stmgr
}  // namespace heron
