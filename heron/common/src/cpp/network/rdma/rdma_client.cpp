#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>
#include <glog/logging.h>

#include "network/rdma/rdma_client.h"
#include "network/rdma/heron_rdma_connection.h"

RDMABaseClient::RDMABaseClient(RDMAOptions *opts, RDMAFabric *rdmaFabric,
                               RDMAEventLoop *loop) {
  this->info_hints = rdmaFabric->GetHints();
  this->eventLoop_ = loop;
  this->options = opts;
  this->eq = NULL;
  this->datagram_ = NULL;
  this->fabric = rdmaFabric->GetFabric();
  // this->info = rdmaFabric->GetInfo();
  this->eq_attr = {};
  this->eq_attr.wait_obj = FI_WAIT_NONE;
  this->eq_attr.size = 64;
  this->conn_ = NULL;
  this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };;
  this->eq_loop.event = CONNECTION;
  this->eq_loop.valid = true;
  this->state_ = INIT;
}

RDMABaseClient::RDMABaseClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMADatagram *loop, uint16_t target_id) {
  this->info_hints = rdmaFabric->GetHints();
  this->datagram_ = loop;
  this->eventLoop_ = NULL;
  this->options = opts;
  this->eq = NULL;
  this->fabric = rdmaFabric->GetFabric();
  // this->info = rdmaFabric->GetInfo();
  this->eq_attr = {};
  this->eq_attr.wait_obj = FI_WAIT_NONE;
  this->eq_attr.size = 64;
  this->conn_ = NULL;
  this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };;
  this->eq_loop.event = CONNECTION;
  this->eq_loop.valid = true;
  this->state_ = INIT;
  this->target_id = target_id;
}

void RDMABaseClient::OnConnect(enum rdma_loop_status state) {
  // LOG(INFO) << "On connect callback";
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;

  if (state_ != CONNECTED && state_ != CONNECTING) {
    LOG(INFO) << "Un-expected state";
    return;
  }

  // read the events for incoming messages
  rd = fi_eq_read(eq, &event, &entry, sizeof entry, 0);
  if (rd == -FI_EAGAIN) {
    return;
  }

  if (rd < 0) {
    if (rd == -FI_EAVAIL) {
      rd = hps_utils_eq_readerr(eq);
      LOG(WARNING) << "Failed to read the eq: " << rd;
    }
    return;
  }

  if (rd != sizeof entry) {
    LOG(INFO) << "Unexpected event received on connection listen "
              << rd << " and expected " << sizeof entry;
    return;
  }

  if (event == FI_SHUTDOWN) {
    Stop_base();
    LOG(INFO) << "Received shutdown event";
  } else if (event == FI_CONNECTED) {
    LOG(INFO) << "Received connected event";
    Connected(&entry);
  } else {
    LOG(ERROR) << "Unexpected CM event" << event;
  }
}

int RDMABaseClient::Stop_base() {
  if (this->state_ != CONNECTED || this->state_ != CONNECTING) {
    LOG(ERROR) << "Trying to stop an un-connected client";
    return 0;
  }

  this->connection_->closeConnection();
  delete connection_;
  HPS_CLOSE_FID(eq);
  this->state_ = DISCONNECTED;

  HPS_CLOSE_FID(eq);

  if (this->options) {
    options->Free();
  }

  if (this->info) {
    fi_freeinfo(this->info);
    this->info = NULL;
  }

  if (conn_) {
    delete conn_;
    conn_ = NULL;
  }

  HandleClose_Base(OK);
  return 0;
}

int RDMABaseClient::CreateConnection() {
  RDMAConnection *con = NULL;
  struct fid_ep *ep = NULL;
  int ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
  if (ret) {
    LOG(ERROR) << "fi_eq_open %d" << ret;
    return ret;
  }

  ret = hps_utils_get_eq_fd(this->options, this->eq, &this->eq_fid);
  if (ret) {
    LOG(ERROR) << "Failed to get event queue fid " << ret;
    return ret;
  }
  LOG(INFO) << "EQ FID: " << eq_fid;
  this->eq_loop.fid = eq_fid;
  this->eq_loop.desc = &this->eq->fid;

  // create the connection
  con = new RDMAConnection(this->options, this->info, this->fabric, this->domain, this->eventLoop_);

  // allocate the resources
  ret = con->SetupQueues();
  if (ret) {
    return ret;
  }

  // create the end point for this connection
  ret = fi_endpoint(domain, this->info, &ep, NULL);
  if (ret) {
    LOG(ERROR) << "fi_endpoint" << ret;
    return ret;
  }

  // initialize the endpoint
  ret = con->InitEndPoint(ep, this->eq);
  if (ret) {
    return ret;
  }

  ret = this->eventLoop_->RegisterRead(&this->eq_loop);
  if (ret) {
    LOG(ERROR) << "Failed to register event queue fid" << ret;
    return ret;
  }


  ret = fi_connect(ep, this->info->dest_addr, NULL, 0);
  if (ret) {
    LOG(ERROR) << "fi_connect %d" << ret;
    return ret;
  }

  this->state_ = CONNECTING;
  this->conn_ = CreateConnection(con, options, this->eventLoop_, READ_WRITE);
  this->connection_ = con;
  LOG(INFO) << "Wating for connection completion";
  return 0;
}

int RDMABaseClient::CreateChannel() {
  LOG(INFO) << "Client info";
  print_info(info);
  channel_ = datagram_->CreateChannel(target_id, info);
  this->conn_ = CreateConnection(channel_, options, this->eventLoop_, WRITE_ONLY);
  LOG(INFO) << "Created channel to stream id: " << target_id;

  int ret = datagram_->SendAddressToRemote(channel_->GetRemoteAddress(), target_id);
  if (ret) {
    LOG(ERROR) << "Failed to send the address to remote: " << target_id;
    return ret;
  }

  this->conn_->start();
  // datagram_->AddChannel(target_id, channel_);
  this->state_ = CONNECTED;
  HandleConnect_Base(OK);
  return 0;
}

int RDMABaseClient::Start_base(void) {
  int ret;
  if (state_ != INIT) {
    LOG(ERROR) << "Failed to start connection not in INIT state";
    return -1;
  }

  ret = hps_utils_get_info_client(options, info_hints, &info);
  if (ret) {
    LOG(ERROR) << "Failed to get server information";
    return ret;
  }

  ret = fi_domain(this->fabric, this->info, &this->domain, NULL);
  if (ret) {
    LOG(ERROR) << "fi_domain " << ret;
    return ret;
  }

  if (options->provider == VERBS_PROVIDER_TYPE) {
    ret = CreateConnection();
    if (ret) {
      LOG(ERROR) << "Failed to create connection " << ret;
      return ret;
    }
  } else if (options->provider == PSM2_PROVIDER_TYPE ) {
    ret = CreateChannel();
    if (ret) {
      LOG(ERROR) << "Failed to create channel " << ret;
      return ret;
     }
  }

  return 0;
}

int RDMABaseClient::Connected(struct fi_eq_cm_entry *entry) {
  if (state_ != CONNECTING) {
    LOG(ERROR) << "Failed to connect a client not in connecting state";
    return -1;
  }

  int ret;
  if (entry->fid != &(this->connection_->GetEp()->fid)) {
    LOG(INFO) << "The fids are not matching";
    return -FI_EOTHER;
  }

  if (conn_->start()) {
    LOG(ERROR) << "Failed to start the connection";
    return 1;
  }

  this->conn_ = conn_;
  this->state_ = CONNECTED;
  LOG(INFO) << "Connection established";
  HandleConnect_Base(OK);
  return 0;
}

bool RDMABaseClient::IsConnected() {
  return state_ == CONNECTED;
}




