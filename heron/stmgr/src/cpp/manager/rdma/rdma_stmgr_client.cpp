#include <stdio.h>
#include <iostream>
#include <string>
#include "basics/sptypes.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/options.h"
#include "network/rdma/heron_rdma_client.h"

#include "rdma_stmgr_client.h"

namespace heron {
namespace stmgr {
RDMAStMgrClient::RDMAStMgrClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric)
    : RDMAClient(_options, fabric, eventLoop),
      quit_(false),
      ndropped_messages_(0) {
  InstallMessageHandler(&RDMAStMgrClient::HandleTupleStreamMessage);
}

RDMAStMgrClient::~RDMAStMgrClient() {
  Stop();
}

void RDMAStMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void RDMAStMgrClient::HandleConnect(NetworkErrorCode _status) {
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


