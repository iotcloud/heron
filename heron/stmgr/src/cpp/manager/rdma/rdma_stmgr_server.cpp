#include "manager/rdma/rdma_stmgr_server.h"
#include <iostream>
#include <set>
#include <vector>

namespace heron {
namespace stmgr {

const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

RDMAStMgrServer::RDMAStMgrServer(RDMAEventLoopNoneFD* eventLoop, RDMAOptions *_options, RDMAFabric *fabric, StMgr *stmgr_)
    : RDMAServer(fabric, eventLoop, _options) {
  // stmgr related handlers
  InstallMessageHandler(&RDMAStMgrServer::HandleTupleStreamMessage);
  LOG(INFO) << "Init server";
  spouts_under_back_pressure_ = false;
  count = 0;
  this->stmgr_ = stmgr_;
}

RDMAStMgrServer::~RDMAStMgrServer() {
  Stop();
}


sp_string RDMAStMgrServer::MakeBackPressureCompIdMetricName(const sp_string& instanceid) {
  return METRIC_TIME_SPENT_BACK_PRESSURE_COMPID + instanceid;
}

void RDMAStMgrServer::HandleNewConnection(HeronRDMAConnection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "Got new connection " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void RDMAStMgrServer::HandleConnectionClose(HeronRDMAConnection* _conn, NetworkErrorCode) {
  LOG(INFO) << "Got connection close of " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void RDMAStMgrServer::HandleTupleStreamMessage(HeronRDMAConnection* _conn,
                                           proto::stmgr::TupleStreamMessage2* _message) {

  delete _message;
}

void RDMAStMgrServer::StartBackPressureConnectionCb(HeronRDMAConnection* _connection) {
  // The connection will notify us when we can stop the back pressure
  _connection->setCausedBackPressure();
}

void RDMAStMgrServer::StopBackPressureConnectionCb(HeronRDMAConnection* _connection) {
  _connection->unsetCausedBackPressure();
}

void RDMAStMgrServer::SendStartBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending start back pressure notification to all other "
            << "stream managers";
  }

void RDMAStMgrServer::SendStopBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending stop back pressure notification to all other "
            << "stream managers";
}

}
}

