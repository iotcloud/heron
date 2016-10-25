#include "heron_rdma_server.h"
#include <string>
#include <utility>
#include <glog/logging.h>

RDMAServer::RDMAServer(RDMAFabric *fabric, RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options)
    : RDMABaseServer(_options, fabric, eventLoop) {
  request_rid_gen_ = new REQID_Generator();
}

RDMAServer::~RDMAServer() { delete request_rid_gen_; }

sp_int32 RDMAServer::Start() { return Start_Base(); }

sp_int32 RDMAServer::Stop() { return Stop_Base(); }

void RDMAServer::SendResponse(REQID _id, HeronRDMAConnection* _connection,
                          const google::protobuf::Message& _response) {
  sp_int32 byte_size = _response.ByteSize();
  sp_uint32 data_size = RDMAOutgoingPacket::SizeRequiredToPackString(_response.GetTypeName()) +
                        REQID_size + RDMAOutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  RDMAOutgoingPacket* opkt = new RDMAOutgoingPacket(data_size);
  CHECK_EQ(opkt->PackString(_response.GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(_id), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_response, byte_size), 0);
  InternalSendResponse(_connection, opkt);
  return;
}

void RDMAServer::SendMessage(HeronRDMAConnection* _connection, const google::protobuf::Message& _message) {
  // Generate a zero reqid
  REQID rid = REQID_Generator::generate_zero_reqid();
  // Currently its no different than response
  return SendResponse(rid, _connection, _message);
}

void RDMAServer::CloseConnection(HeronRDMAConnection* _connection) { CloseConnection_Base(_connection); }

void RDMAServer::AddTimer(VCallback<> cb, sp_int64 _msecs) {  }

void RDMAServer::SendRequest(HeronRDMAConnection* _conn, google::protobuf::Message* _request, void* _ctx,
                         google::protobuf::Message* _response_placeholder) {
  SendRequest(_conn, _request, _ctx, -1, _response_placeholder);
}

void RDMAServer::SendRequest(HeronRDMAConnection* _conn, google::protobuf::Message* _request, void* _ctx,
                         sp_int64 _msecs, google::protobuf::Message* _response_placeholder) {
  InternalSendRequest(_conn, _request, _msecs, _response_placeholder, _ctx);
}

// The interfaces of BaseServer being implemented
RDMABaseConnection* RDMAServer::CreateConnection(RDMAConnection* endpoint, RDMAOptions* options,
                                         RDMAEventLoopNoneFD* ss) {
  // Create the connection object and register our callbacks on various events.
  HeronRDMAConnection* conn = new HeronRDMAConnection(options, endpoint, ss);
  auto npcb = [conn, this](RDMAIncomingPacket* packet) { this->OnNewPacket(conn, packet); };
  conn->registerForNewPacket(npcb);

  // Backpressure reliever - will point to the inheritor of this class in case the virtual function
  // is implemented in the inheritor
  auto backpressure_reliever_ = [this](HeronRDMAConnection* cn) {
    this->StopBackPressureConnectionCb(cn);
  };

  auto backpressure_starter_ = [this](HeronRDMAConnection* cn) {
    this->StartBackPressureConnectionCb(cn);
  };

  conn->registerForBackPressure(std::move(backpressure_starter_),
                                std::move(backpressure_reliever_));
  return conn;
}

void RDMAServer::HandleNewConnection_Base(RDMABaseConnection* _connection) {
  HandleNewConnection(static_cast<HeronRDMAConnection*>(_connection));
}

void RDMAServer::HandleConnectionClose_Base(RDMABaseConnection* _connection, NetworkErrorCode _status) {
  HandleConnectionClose(static_cast<HeronRDMAConnection*>(_connection), _status);
}

void RDMAServer::OnNewPacket(HeronRDMAConnection* _connection, RDMAIncomingPacket* _packet) {
  // Maybe we can could the number of packets received by each connection?
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Packet Received on on unknown connection " << _connection << " from hostport "
               << _connection->getIPAddress() << ":" << _connection->getPort();
    delete _packet;
    _connection->closeConnection();
    return;
  }

  std::string typname;
  if (_packet->UnPackString(&typname) != 0) {
    LOG(ERROR) << "UnPackString failed from connection " << _connection << " from hostport "
               << _connection->getIPAddress() << ":" << _connection->getPort();
    delete _packet;
    _connection->closeConnection();
    return;
  }
  if (requestHandlers.count(typname) > 0) {
    // This is a request
    requestHandlers[typname](_connection, _packet);
  } else if (messageHandlers.count(typname) > 0) {
    // This is a message
    messageHandlers[typname](_connection, _packet);
  } else {
    // This might be a response for a send request
    REQID rid;
    CHECK_EQ(_packet->UnPackREQID(&rid), 0);
    if (context_map_.find(rid) != context_map_.end()) {
      // Yes this is indeed a good packet
      std::pair<google::protobuf::Message*, void*> pr = context_map_[rid];
      context_map_.erase(rid);
      NetworkErrorCode status = OK;
      if (_packet->UnPackProtocolBuffer(pr.first) != 0) {
        status = INVALID_PACKET;
      }
      auto cb = [pr, status, this]() { this->HandleResponse(pr.first, pr.second, status); };

      AddTimer(std::move(cb), 0);
    } else {
      // This is some unknown message
      LOG(ERROR) << "Unknown type protobuf received " << typname << " deleting...";
    }
  }
  delete _packet;
}

// Backpressure here - works for sending to both worker and stmgr
void RDMAServer::InternalSendResponse(HeronRDMAConnection* _connection, RDMAOutgoingPacket* _packet) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Trying to send on unknown connection! Dropping.. " << std::endl;
    delete _packet;
    return;
  }
  if (_connection->sendPacket(_packet, NULL) != 0) {
    LOG(ERROR) << "Error sending packet to! Dropping... " << std::endl;
    delete _packet;
    return;
  }
  return;
}

void RDMAServer::InternalSendRequest(HeronRDMAConnection* _conn, google::protobuf::Message* _request,
                                 sp_int64 _msecs, google::protobuf::Message* _response_placeholder,
                                 void* _ctx) {
  if (active_connections_.find(_conn) == active_connections_.end()) {
    delete _request;
    auto cb = [_response_placeholder, _ctx, this]() {
      this->HandleResponse(_response_placeholder, _ctx, WRITE_ERROR);
    };
    AddTimer(std::move(cb), 0);
    return;
  }

  // Generate the rid.
  REQID rid = request_rid_gen_->generate();

  // Insert into context map
  // TODO(kramasamy): If connection closes and there is no timeout associated with
  // a request, then the context_map_ will forever be left dangling.
  // One way to solve this would be to always have a timeout associated
  context_map_[rid] = std::make_pair(_response_placeholder, _ctx);

  // Make the outgoing packet
  sp_int32 byte_size = _request->ByteSize();
  sp_uint32 sop = RDMAOutgoingPacket::SizeRequiredToPackString(_request->GetTypeName()) + REQID_size +
                  RDMAOutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  RDMAOutgoingPacket* opkt = new RDMAOutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_request->GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(*_request, byte_size), 0);

  // delete the request
  delete _request;

  if (_conn->sendPacket(opkt, NULL) != 0) {
    context_map_.erase(rid);
    delete opkt;
    auto cb = [_response_placeholder, _ctx, this]() {
      this->HandleResponse(_response_placeholder, _ctx, WRITE_ERROR);
    };
    AddTimer(std::move(cb), 0);
    return;
  }
  if (_msecs > 0) {
    auto cb = [rid, this](RDMAEventLoopNoneFD::Status status) { this->OnPacketTimer(rid, status); };
    CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, _msecs), 0);
  }
  return;
}

void RDMAServer::OnPacketTimer(REQID _id, RDMAEventLoopNoneFD::Status) {
  if (context_map_.find(_id) == context_map_.end()) {
    // most likely this was due to the requests being retired before the timer.
    return;
  }

  std::pair<google::protobuf::Message*, void*> pr = context_map_[_id];
  context_map_.erase(_id);

  HandleResponse(pr.first, pr.second, TIMEOUT);
}

// default implementation
void RDMAServer::HandleResponse(google::protobuf::Message* _response, void*, NetworkErrorCode) {
  delete _response;
}

void RDMAServer::StartBackPressureConnectionCb(HeronRDMAConnection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}

void RDMAServer::StopBackPressureConnectionCb(HeronRDMAConnection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}
