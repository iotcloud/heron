#include <stdio.h>
#include <iostream>
#include <string>
#include "basics/sptypes.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/options.h"
#include "network/rdma/heron_rdma_client.h"
#include "config/heron-internals-config-reader.h"
#include "manager/rdma/rdma_stmgr_client.h"

namespace heron {
namespace stmgr {
RDMAStMgrClient::RDMAStMgrClient(RDMAEventLoop* rdmaEventLoop, EventLoop* eventLoop,
                                   RDMAOptions* _options, RDMAFabric *fabric,
                                   const sp_string& _topology_name, const sp_string& _topology_id,
                                   const sp_string& _our_id,
                                   const sp_string& _other_id,StMgrClientMgr* _client_manager)
    : RDMAClient(_options, fabric, rdmaEventLoop),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      our_stmgr_id_(_our_id),
      other_stmgr_id_(_other_id),
      quit_(false),
      client_manager_(_client_manager),
      nEventLoop_(eventLoop),
      ndropped_messages_(0) {
  reconnect_other_streammgrs_interval_sec_ =
        config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectIntervalSec();
  InstallMessageHandler(&RDMAStMgrClient::HandleTupleStreamMessage);
  InstallResponseHandler(new proto::stmgr::StrMgrHelloRequest(), &RDMAStMgrClient::HandleHelloResponse);
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
    LOG(INFO) << "Connected to stmgr " << other_stmgr_id_ << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendHelloRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to stmgr " << other_stmgr_id_ << std::endl;
    if (quit_) {
      LOG(ERROR) << "Quitting";
      delete this;
      return;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
      AddTimerStmgr([this]() { this->OnReConnectTimer(); },
               reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
    }
  }
}

void RDMAStMgrClient::HandleClose(NetworkErrorCode _code) {
  if (quit_) {
    LOG(INFO) << "Closing and quitting";
    delete this;
  } else {
    LOG(INFO) << "Closed and Will try to reconnect again after 1 seconds" << std::endl;
    AddTimerStmgr([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void RDMAStMgrClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                      NetworkErrorCode _status) {
  LOG(INFO) << "Got Hello reponse";
  if (_status != OK) {
    LOG(ERROR) << "NonOK network code " << _status << " for register response from stmgr "
               << other_stmgr_id_;
    delete _response;
    Stop();
    return;
  }
  proto::system::StatusCode status = _response->status().status();
  if (status != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << status << " from stmgr " << other_stmgr_id_;
    Stop();
  }
  delete _response;
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
    // LOG(INFO) << "Sending message";
    SendMessage(_msg);
  } else {
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
      << other_stmgr_id_ << " because it is not connected";
    }
    // delete _msg;
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

sp_int64 RDMAStMgrClient::AddTimerStmgr(VCallback<> cb, sp_int64 _msecs) {
  auto eventCb = [cb, this](EventLoop::Status status) { this->OnTimer(std::move(cb), status); };

  sp_int64 timer_id = nEventLoop_->registerTimer(std::move(eventCb), false, _msecs);
  CHECK_GT(timer_id, 0);
  return timer_id;
}

void RDMAStMgrClient::OnTimer(VCallback<> cb, EventLoop::Status) { cb(); }

}
}


