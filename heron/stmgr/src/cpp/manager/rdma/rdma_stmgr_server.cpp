#include "heron_stmgr_server.h"
#include <iostream>
#include <set>
#include <vector>
#include <heron_rdma_server.h>

const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

StMgrRDMAServer::StMgrRDMAServer(RDMAEventLoopNoneFD* eventLoop, RDMAOptions *_options, RDMAFabric *fabric)
    : Server(fabric, eventLoop, _options) {
  // stmgr related handlers
  InstallMessageHandler(&StMgrRDMAServer::HandleTupleStreamMessage);
  LOG(INFO) << "Init server";
  spouts_under_back_pressure_ = false;
  count = 0;
}

StMgrRDMAServer::~StMgrRDMAServer() {
  Stop();
}


sp_string StMgrRDMAServer::MakeBackPressureCompIdMetricName(const sp_string& instanceid) {
  return METRIC_TIME_SPENT_BACK_PRESSURE_COMPID + instanceid;
}

void StMgrRDMAServer::HandleNewConnection(Connection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "Got new connection " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void StMgrRDMAServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  LOG(INFO) << "Got connection close of " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void StMgrRDMAServer::HandleTupleStreamMessage(Connection* _conn,
                                           proto::stmgr::TupleMessage* _message) {
  LOG(INFO) << _message->id() << " " << _message->data();
  if (_message->id() == -1) {
    count = 0;
  } else {
    if (count != _message->id()) {
      LOG(ERROR) << "Invalid message sequence, count: " << count << " id: " << _message->id();
    }
    count++;
  }

  char *name = new char[100];
  sprintf(name, "Hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii");
  proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
  message->set_name(name);
  message->set_id(10);
  message->set_data(name);
  message->set_time(_message->time());

  SendMessage(_conn, (*message));
  delete message;
  delete _message;

  //printf("%d\n", (count % 1000));
  if ((count % 10000) == 0) {
    printf("count %d\n", count);
  }
}

void StMgrRDMAServer::StartBackPressureConnectionCb(Connection* _connection) {
  // The connection will notify us when we can stop the back pressure
  _connection->setCausedBackPressure();
}

void StMgrRDMAServer::StopBackPressureConnectionCb(Connection* _connection) {
  _connection->unsetCausedBackPressure();
}

void StMgrRDMAServer::SendStartBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending start back pressure notification to all other "
            << "stream managers";
  }

void StMgrRDMAServer::SendStopBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending stop back pressure notification to all other "
            << "stream managers";
}


