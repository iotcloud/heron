#ifndef RDMA_CLIENT_H_
#define RDMA_CLIENT_H_

#include "network/rdma/utils.h"
#include "network/rdma/options.h"
#include "network/rdma/rdma_connection.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/rdma_fabric.h"
#include "network/rdma/rdma_base_connection.h"
#include "network/network_error.h"

class RDMABaseClient {
public:
  enum ClientState {INIT = 0, DISCONNECTED, CONNECTING, CONNECTED };
  RDMABaseClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop);
  int Start_base(void);
  int Stop_base();
  // Instantiate a new connection
  virtual RDMABaseConnection* CreateConnection(RDMAConnection* endpoint, RDMAOptions* options,
                                           RDMAEventLoopNoneFD* ss) = 0;
  void OnConnect(enum rdma_loop_status state);
  bool IsConnected();
protected:
// Derived class should implement this method to handle Connection
  // establishment. a status of OK implies that the Client was
  // successful in connecting to hte client. Requests can now be sent to
  // the server. Any other status implies that the connect failed.
  virtual void HandleConnect_Base(NetworkErrorCode status) = 0;

  // When the underlying socket is closed(either because of an explicit
  // Stop done by derived class or because a read/write failed and
  // the connection closed automatically on us), this method is
  // called. A status of OK means that this was a user initiated
  // Close that successfully went through. A status value of
  // READ_ERROR implies that there was problem reading in the
  // connection and thats why the connection was closed internally.
  // A status value of WRITE_ERROR implies that there was a problem writing
  // in the connection. Derived classes can do any cleanups in this method.
  virtual void HandleClose_Base(NetworkErrorCode status) = 0;

  RDMABaseConnection *conn_;
  // the connection
  RDMAConnection *connection_;
  RDMAEventLoopNoneFD *eventLoop_;
  ClientState state_;
private:
  // options for initialization
  RDMAOptions *options;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the event queue to for  connection handling
  struct fid_eq *eq;
  // the loop callback
  struct rdma_loop_info eq_loop;
  // the file descriptor for eq
  int eq_fid;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;


  int Connected(fi_eq_cm_entry *entry);
};

#endif /* SCLIENT_H_ */