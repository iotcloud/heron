#include "manager/rdma/rdma_stmgr_server.h"
#include "manager/stmgr.h"
#include <iostream>
#include <set>
#include <vector>

namespace heron {
namespace stmgr {

const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

RDMAStMgrServer::RDMAStMgrServer(RDMAEventLoopNoneFD* eventLoop, RDMAOptions *_options, RDMAFabric *fabric,
const sp_string& _topology_name, const sp_string& _topology_id,
const sp_string& _stmgr_id, StMgr *stmgr_)
    : RDMAServer(fabric, eventLoop, _options), topology_name_(_topology_name), topology_id_(_topology_id), stmgr_id_(_stmgr_id), stmgr_(stmgr_) {
  // stmgr related handlers
  InstallMessageHandler(&RDMAStMgrServer::HandleTupleStreamMessage);
  InstallRequestHandler(&RDMAStMgrServer::HandleStMgrHelloRequest);
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

void RDMAStMgrServer::HandleStMgrHelloRequest(REQID _id, HeronRDMAConnection* _conn,
                                          proto::stmgr::StrMgrHelloRequest* _request) {
  LOG(INFO) << "Got a hello message from stmgr " << _request->stmgr() << " on connection " << _conn;
  proto::stmgr::StrMgrHelloResponse response;
  // Some basic checks
  if (_request->topology_name() != topology_name_) {
    LOG(ERROR) << "The hello message was from a different topology " << _request->topology_name()
               << std::endl;
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else if (_request->topology_id() != topology_id_) {
    LOG(ERROR) << "The hello message was from a different topology id " << _request->topology_id()
               << std::endl;
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else if (stmgrs_.find(_request->stmgr()) != stmgrs_.end()) {
    LOG(WARNING) << "We already had an active connection from the stmgr " << _request->stmgr()
                 << ". Closing existing connection...";
    // This will free up the slot in the various maps in this class
    // and the next time around we'll be able to add this stmgr.
    // We shouldn't add the new stmgr connection right now because
    // the close could be asynchronous (fired through a 0 timer)
    stmgrs_[_request->stmgr()]->closeConnection();
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else {
    stmgrs_[_request->stmgr()] = _conn;
    rstmgrs_[_conn] = _request->stmgr();
    response.mutable_status()->set_status(proto::system::OK);
  }
  SendResponse(_id, _conn, response);
  delete _request;
}

void RDMAStMgrServer::HandleTupleStreamMessage(HeronRDMAConnection* _conn,
                                           proto::stmgr::TupleStreamMessage2* _message) {
  auto iter = rstmgrs_.find(_conn);
  if (iter == rstmgrs_.end()) {
    LOG(INFO) << "Recieved Tuple messages from unknown streammanager connection" << std::endl;
  } else {
    // LOG(INFO) << "Recieved Tuple message.." << std::endl;
    stmgr_->HandleStreamManagerData(iter->second, *_message);
  }
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

