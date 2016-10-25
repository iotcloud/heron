#include <stdio.h>
#include <iostream>
#include <string>
#include "basics/sptypes.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/options.h"
#include "network/rdma/heron_rdma_client.h"

#include "heron_client.h"

StMgrRDMAClient::StMgrRDMAClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric)
    : Client(_options, fabric, eventLoop),
      quit_(false),
      ndropped_messages_(0) {
  InstallMessageHandler(&StMgrRDMAClient::HandleTupleStreamMessage);
}

StMgrRDMAClient::~StMgrRDMAClient() {
  Stop();
}

void StMgrRDMAClient::Quit() {
  quit_ = true;
  Stop();
}

void StMgrRDMAClient::HandleConnect(NetworkErrorCode _status) {
}

void StMgrRDMAClient::HandleClose(NetworkErrorCode _code) {
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void StMgrRDMAClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                      NetworkErrorCode _status) {
}

void StMgrRDMAClient::OnReConnectTimer() { Start(); }

void StMgrRDMAClient::SendHelloRequest() {
  return;
}

void StMgrRDMAClient::SendTupleStreamMessage(proto::stmgr::TupleStreamMessage2& _msg) {
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

void StMgrRDMAClient::HandleTupleStreamMessage(proto::stmgr::TupleMessage* _message) {
  //LOG(INFO) << _message->id() << " " << _message->data();
  Timer timer;
  count++;
  if (count % 10000 == 0) {
    double t = _message->time();
     printf("count: %d time: %lf\n ", count, (timer.currentTime() - t));
  }
  delete _message;
}

void StMgrRDMAClient::StartBackPressureConnectionCb(Connection* _connection) {
}

void StMgrRDMAClient::StopBackPressureConnectionCb(Connection* _connection) {
}


