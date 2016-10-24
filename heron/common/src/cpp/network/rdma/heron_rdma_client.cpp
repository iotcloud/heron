#include "heron_rdma_client.h"
#include "ridgen.h"
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>

Client::Client(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop)
    : RDMABaseClient(opts, rdmaFabric, loop) {
  Init();
}

Client::~Client() { delete message_rid_gen_; }

void Client::Start() { Start_base(); }

void Client::Stop() { Stop_base(); }

void Client::SendRequest(google::protobuf::Message* _request, void* _ctx) {
  SendRequest(_request, _ctx, -1);
}

void Client::SendRequest(google::protobuf::Message* _request, void* _ctx, sp_int64 _msecs) {
  InternalSendRequest(_request, _ctx, _msecs);
}

void Client::SendResponse(REQID _id, const google::protobuf::Message& _response) {
  sp_int32 byte_size = _response.ByteSize();
  sp_uint32 data_size = OutgoingPacket::SizeRequiredToPackString(_response.GetTypeName()) +
                        REQID_size + OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  OutgoingPacket* opkt = new OutgoingPacket(data_size);
  CHECK_EQ(opkt->PackString(_response.GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(_id), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_response, byte_size), 0);
  InternalSendResponse(opkt);
  return;
}

void Client::SendMessage(google::protobuf::Message* _message) { 
  // LOG(INFO) << "Send request";
  InternalSendMessage(_message); 
}

sp_int64 Client::AddTimer(VCallback<> cb, sp_int64 _msecs) {
  return 0;
}

sp_int32 Client::RemoveTimer(sp_int64 timer_id) { return 0;}

BaseConnection* Client::CreateConnection(RDMAConnection* endpoint, RDMAOptions* options,
                                         RDMAEventLoop* ss) {
  Connection* conn = new Connection(options, endpoint, ss);

  conn->registerForNewPacket([this](IncomingPacket* pkt) { this->OnNewPacket(pkt); });
  // Backpressure reliever - will point to the inheritor of this class in case the virtual function
  // is implemented in the inheritor
  auto backpressure_reliever_ = [this](Connection* cn) {
    this->StopBackPressureConnectionCb(cn);
  };

  auto backpressure_starter_ = [this](Connection* cn) {
    this->StartBackPressureConnectionCb(cn);
  };

  conn->registerForBackPressure(std::move(backpressure_starter_),
                                std::move(backpressure_reliever_));
  return conn;
}

void Client::HandleConnect_Base(NetworkErrorCode _status) { HandleConnect(_status); }

void Client::HandleClose_Base(NetworkErrorCode _status) { HandleClose(_status); }

void Client::Init() { message_rid_gen_ = new REQID_Generator(); }

void Client::InternalSendRequest(google::protobuf::Message* _request, void* _ctx, sp_int64 _msecs) {
  CHECK(requestResponseMap_.find(_request->GetTypeName()) != requestResponseMap_.end());
  // LOG(INFO) << "Internal send request";
  const sp_string& _expected_response_type = requestResponseMap_[_request->GetTypeName()];
  if (state_ != CONNECTED) {
    delete _request;
    responseHandlers[_expected_response_type](NULL, WRITE_ERROR);
    return;
  }

  // Generate the rid.
  REQID rid = message_rid_gen_->generate();

  // Insert into context map
  context_map_[rid] = std::make_pair(_expected_response_type, _ctx);

  // Make the outgoing packet
  sp_int32 byte_size = _request->ByteSize();
  sp_uint32 sop = OutgoingPacket::SizeRequiredToPackString(_request->GetTypeName()) + REQID_size +
                  OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  OutgoingPacket* opkt = new OutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_request->GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(*_request, byte_size), 0);

  // delete the request
  delete _request;

  Connection* conn = static_cast<Connection*>(conn_);
  if (conn->sendPacket(opkt, NULL) != 0) {
    context_map_.erase(rid);
    delete opkt;
    responseHandlers[_expected_response_type](NULL, WRITE_ERROR);
    return;
  }
  if (_msecs > 0) {
    auto cb = [rid, this](RDMAEventLoop::Status s) { this->OnPacketTimer(rid, s); };
    CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, _msecs), 0);
  }
  return;
}

void Client::InternalSendMessage(google::protobuf::Message* _message) {
//  LOG(INFO) << "Internal send message";
  if (state_ != CONNECTED) {
    LOG(ERROR) << "Client is not connected. Dropping message" << std::endl;
    delete _message;
    return;
  }

  // Generate a zero rid.
  REQID rid = REQID_Generator::generate_zero_reqid();

  // Make the outgoing packet
  sp_int32 byte_size = _message->ByteSize();
  sp_uint32 sop = OutgoingPacket::SizeRequiredToPackString(_message->GetTypeName()) + REQID_size +
                  OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  OutgoingPacket* opkt = new OutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_message->GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(*_message, byte_size), 0);

  // delete the message
  delete _message;

  Connection* conn = static_cast<Connection*>(conn_);
  // LOG(INFO) << "Send message";
  if (conn->sendPacket(opkt, NULL) != 0) {
    LOG(ERROR) << "Some problem sending message thru the connection. Dropping message" << std::endl;
    delete opkt;
    return;
  }
  return;
}

void Client::InternalSendResponse(OutgoingPacket* _packet) {
  if (state_ != CONNECTED) {
    LOG(ERROR) << "Client is not connected. Dropping response" << std::endl;
    delete _packet;
    return;
  }

  Connection* conn = static_cast<Connection*>(conn_);
  if (conn->sendPacket(_packet, NULL) != 0) {
    LOG(ERROR) << "Error sending packet to! Dropping..." << std::endl;
    delete _packet;
    return;
  }
  return;
}

void Client::OnNewPacket(IncomingPacket* _ipkt) {
  std::string typname;

  if (_ipkt->UnPackString(&typname) != 0) {
    Connection* conn = static_cast<Connection*>(conn_);
    LOG(FATAL) << "UnPackString failed from connection " << conn << " from hostport "
               << conn->getIPAddress() << ":" << conn->getPort();
  }

  if (requestHandlers.count(typname) > 0) {
    // this is a request
    requestHandlers[typname](_ipkt);
  } else if (messageHandlers.count(typname) > 0) {
    // This is a message
    // We just ignore the reqid
    REQID rid;
    CHECK_EQ(_ipkt->UnPackREQID(&rid), 0);
    // This is a message
    messageHandlers[typname](_ipkt);
  } else if (responseHandlers.count(typname) > 0) {
    // This is a response
    responseHandlers[typname](_ipkt, OK);
  }
  delete _ipkt;
}

void Client::OnPacketTimer(REQID _id, RDMAEventLoop::Status) {
  if (context_map_.find(_id) == context_map_.end()) {
    // most likely this was due to the requests being retired before the timer.
    return;
  }

  const string& expected_response_type = context_map_[_id].first;
  responseHandlers[expected_response_type](NULL, TIMEOUT);
}

void Client::StartBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}

void Client::StopBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}
