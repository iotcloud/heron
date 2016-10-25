#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_

#include <map>
#include <set>
#include <vector>
#include "basics/sptypes.h"
#include <connection.h>
#include <heron_rdma_server.h>
#include <message.pb.h>
#include <network_error.h>

namespace heron {
  namespace common {
    class MetricsMgrSt;

    class MultiCountMetric;

    class TimeSpentMetric;
  }
}


class StMgr;

class StMgrRDMAServer : public Server {
public:
StMgrRDMAServer(RDMAEventLoopNoneFD* eventLoop, RDMAOptions *_options, RDMAFabric *fabric);

virtual ~StMgrRDMAServer();

bool HaveAllInstancesConnectedToUs() const {
  return active_instances_.size() == expected_instances_.size();
}


protected:
virtual void HandleNewConnection(Connection *newConnection);

virtual void HandleConnectionClose(Connection *connection, NetworkErrorCode status);

private:
sp_string MakeBackPressureCompIdMetricName(const sp_string &instanceid);

void HandleTupleStreamMessage(Connection *_conn, proto::stmgr::TupleMessage *_message);

// Backpressure message from and to other stream managers
void SendStartBackPressureToOtherStMgrs();

void SendStopBackPressureToOtherStMgrs();

// Back pressure related connection callbacks
// Do back pressure
void StartBackPressureConnectionCb(Connection *_connection);

// Relieve back pressure
void StopBackPressureConnectionCb(Connection *_connection);

// map from stmgr_id to their connection
typedef std::map<sp_string, Connection *> StreamManagerConnectionMap;
StreamManagerConnectionMap stmgrs_;
// Same as above but reverse
typedef std::map<Connection *, sp_string> ConnectionStreamManagerMap;
ConnectionStreamManagerMap rstmgrs_;

// map from Connection to their task_id
typedef std::map<Connection *, sp_int32> ConnectionTaskIdMap;
ConnectionTaskIdMap active_instances_;
// map of Instance_id/stmgrid to metric
// Used for back pressure metrics
typedef std::map<sp_string, heron::common::TimeSpentMetric *> InstanceMetricMap;
InstanceMetricMap instance_metric_map_;

// instances/stream mgrs causing back pressure
std::set<sp_string> remote_ends_who_caused_back_pressure_;
// stream managers that have announced back pressure
std::set<sp_string> stmgrs_who_announced_back_pressure_;

sp_string topology_name_;
sp_string topology_id_;
sp_string stmgr_id_;
std::vector<sp_string> expected_instances_;
int count;

bool spouts_under_back_pressure_;
};

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_