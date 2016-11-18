#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_

#include "network/rdma/heron_rdma_client.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network_error.h"
#include "network/packet.h"

namespace heron {
namespace stmgr {
class StMgrClientMgr;

class RDMAStMgrClient : public RDMAClient {
public:
  RDMAStMgrClient(RDMAEventLoopNoneFD* rdmaEventLoop, EventLoop* eventLoop,
  RDMAOptions* _options, RDMAFabric *fabric,
  const sp_string& _topology_name, const sp_string& _topology_id, const sp_string& _our_id,
  const sp_string& _other_id, StMgrClientMgr* _client_manager);
  virtual ~RDMAStMgrClient();

  void Quit();

  void SendTupleStreamMessage(proto::stmgr::TupleStreamMessage2* _msg);

protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

private:
  void HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response, NetworkErrorCode);
  void HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage2* _message);

  void OnReConnectTimer();
  void SendHelloRequest();
  // Do back pressure
  virtual void StartBackPressureConnectionCb(HeronRDMAConnection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(HeronRDMAConnection* _connection);
  void OnTimer(VCallback<> cb, EventLoop::Status);
  sp_int64 AddTimerStmgr(VCallback<> cb, sp_int64 _msecs);

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string our_stmgr_id_;
  sp_string other_stmgr_id_;
  bool quit_;
  EventLoop* nEventLoop_;
  // Configs to be read
  sp_int32 reconnect_other_streammgrs_interval_sec_;

  // Counters
  sp_int64 ndropped_messages_;

  StMgrClientMgr* client_manager_;
};

}
}

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_