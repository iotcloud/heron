#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_

#include "network/rdma/heron_rdma_client.h"

class StMgrClientMgr;

class StMgrRDMAClient : public Client {
public:
  StMgrRDMAClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric);
  virtual ~StMgrRDMAClient();

  void Quit();

  void SendTupleStreamMessage(proto::stmgr::TupleMessage* _msg);

protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

private:
  void HandleHelloResponse(void*, proto::stmgr::TupleMessage* _response, NetworkErrorCode);
  void HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage2& _msg);

  void OnReConnectTimer();
  void SendHelloRequest();
  // Do back pressure
  virtual void StartBackPressureConnectionCb(Connection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  sp_string other_stmgr_id_;
  bool quit_;
  uint32_t count;

  // Configs to be read
  sp_int32 reconnect_other_streammgrs_interval_sec_;

  // Counters
  sp_int64 ndropped_messages_;
};

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_RDMA_CLIENT_H_