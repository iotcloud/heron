#include <stdio.h>
#include <iostream>
#include <string>
#include "basics/sptypes.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/options.h"
#include "network/rdma/heron_rdma_client.h"

#include "manager/rdma/rdma_stmgr_client.h"

namespace heron {
namespace stmgr {
RDMAStMgrClient::RDMAStMgrClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric,
                                   const sp_string& _topology_name, const sp_string& _topology_id,
                                   const sp_string& _our_id,
                                   const sp_string& _other_id,StMgrClientMgr* _client_manager)
    : RDMAClient(_options, fabric, eventLoop),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      our_stmgr_id_(_our_id),
      other_stmgr_id_(_other_id),
      quit_(false),
      client_manager_(_client_manager),
      ndropped_messages_(0) {
  InstallMessageHandler(&RDMAStMgrClient::HandleTupleStreamMessage);
  this->client_manager_ = _client_manager;
}

RDMAStMgrClient::~RDMAStMgrClient() {
  Stop();
}

void RDMAStMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void RDMAStMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
//    LOG(INFO) << "Connected to stmgr " << other_stmgr_id_ << " running at "
//              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
//              << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendHelloRequest();
    }
  } else {
//    LOG(WARNING) << "Could not connect to stmgr " << other_stmgr_id_ << " running at "
//                 << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
//                 << " due to: " << _status << std::endl;
    if (quit_) {
      LOG(ERROR) << "Quitting";
      delete this;
      return;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
//      AddTimer([this]() { this->OnReConnectTimer(); },
//               reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
    }
  }
}

void RDMAStMgrClient::HandleClose(NetworkErrorCode _code) {
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void RDMAStMgrClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                      NetworkErrorCode _status) {
}

void RDMAStMgrClient::OnReConnectTimer() { Start(); }

void RDMAStMgrClient::SendHelloRequest() {
  auto request = new proto::stmgr::StrMgrHelloRequest();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr(our_stmgr_id_);
  SendRequest(request, NULL);
  return;
}

void RDMAStMgrClient::SendTupleStreamMessage(proto::stmgr::TupleStreamMessage2* _msg) {
  if (IsConnected()) {
    // LOG(INFO) << "Send message";
    SendMessage(_msg);
  } else {
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
      << other_stmgr_id_ << " because it is not connected";
    }
    delete _msg;
  }
}

void RDMAStMgrClient::HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage2* _message) {
  //LOG(INFO) << _message->id() << " " << _message->data();
  delete _message;
}

void RDMAStMgrClient::StartBackPressureConnectionCb(HeronRDMAConnection* _connection) {
}

void RDMAStMgrClient::StopBackPressureConnectionCb(HeronRDMAConnection* _connection) {
}

}
}


