#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_RDMA_STMGR_SERVER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_RDMA_STMGR_SERVER_H_

#include <map>
#include <set>
#include <vector>
#include "basics/sptypes.h"
#include "network/rdma/heron_rdma_connection.h"
#include "network/rdma/heron_rdma_server.h"

#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
  namespace common {
    class MetricsMgrSt;

    class MultiCountMetric;

    class TimeSpentMetric;
  }
}

namespace heron {
namespace stmgr {

class StMgr;

class RDMAStMgrServer : public RDMAServer {
public:
  RDMAStMgrServer(RDMAEventLoopNoneFD *eventLoop, RDMAOptions *_options, RDMAFabric *fabric,
  const sp_string& _topology_name, const sp_string& _topology_id,
  const sp_string& _stmgr_id, StMgr *stmgr_);

  virtual ~RDMAStMgrServer();

protected:
  virtual void HandleNewConnection(HeronRDMAConnection *newConnection);

  virtual void HandleConnectionClose(HeronRDMAConnection *connection, NetworkErrorCode status);

  void HandleStMgrHelloRequest(REQID _id, HeronRDMAConnection* _conn,
                                            proto::stmgr::StrMgrHelloRequest* _request);

private:
  sp_string MakeBackPressureCompIdMetricName(const sp_string &instanceid);

  void HandleTupleStreamMessage(HeronRDMAConnection *_conn, proto::stmgr::TupleStreamMessage2* _message);

// Backpressure message from and to other stream managers
  void SendStartBackPressureToOtherStMgrs();

  void SendStopBackPressureToOtherStMgrs();

// Back pressure related connection callbacks
// Do back pressure
  void StartBackPressureConnectionCb(HeronRDMAConnection *_connection);

// Relieve back pressure
  void StopBackPressureConnectionCb(HeronRDMAConnection *_connection);

// map from stmgr_id to their connection
  typedef std::map<sp_string, HeronRDMAConnection *> StreamManagerConnectionMap;
  StreamManagerConnectionMap stmgrs_;
// Same as above but reverse
  typedef std::map<HeronRDMAConnection *, sp_string> ConnectionStreamManagerMap;
  ConnectionStreamManagerMap rstmgrs_;

// instances/stream mgrs causing back pressure
  std::set<sp_string> remote_ends_who_caused_back_pressure_;
// stream managers that have announced back pressure
  std::set<sp_string> stmgrs_who_announced_back_pressure_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  std::vector<sp_string> expected_instances_;
  int count;
  StMgr *stmgr_;

  bool spouts_under_back_pressure_;
};
}
}

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_