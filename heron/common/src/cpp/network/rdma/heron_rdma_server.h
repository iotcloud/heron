#ifndef HERON_RDMA_SERVER_H
#define HERON_RDMA_SERVER_H

#include <iostream>
#include <glog/logging.h>
#include "rdma_event_loop.h"
#include "rdma_base_connection.h"
#include "connection.h"
#include "rdma_server.h"
#include "ridgen.h"

/*
 * Server class definition
 * Given a host/port, the server binds and listens on that host/port
 * It calls various virtual methods when events happen. The events are
 * HandleNewConnection:- Upon a new connection accept, we invoke
 *                       this method. In this method derived classes can
 *                       send greeting messages, close the connection, etc
 * HandleConnectionClose:- We call this function after closing the connection.
 *                         Derived classes can cleanup stuff in this method
 * HandleNewRequest:- Whenever a new packet arrives, we call this method.
 *                   Derived classes can parse stuff in this method.
 * HandleSentResponse:- Whenever we send a response to the client, we call
 *                   this method. Derived classes can update their state, etc.
 * Derived classes can use the SendResponse method to send a packet.
 * They can use the CloseConnection to explicitly close a connection.
 * Note that during this method, the Server will call the
 * HandleConnectionClose as well.
 */
class Server : public RDMABaseServer {
public:
  // Constructor
  // The Constructor simply inits the member variable.
  // Users must call Start method to start sending/receiving packets.
  Server(RDMAFabric *fabric, RDMAEventLoop* eventLoop, RDMAOptions *_options);

  // Destructor.
  virtual ~Server();

  // Start listening on the host port pair for new requests
  // A zero return value means success. A negative value implies
  // that the server could not bind/listen on the port.
  int32_t Start();

  // Close all active connections and stop listening.
  // A zero return value means success. No more new connections
  // will be accepted from this point onwards. All active
  // connections will be closed. This might result in responses
  // that were sent using SendResponse but not yet acked by HandleSentResponse
  // being discarded.
  // A negative return value implies some error happened.
  int32_t Stop();

  // Send a response back to the client. This is the primary way of
  // communicating with the client.
  // When the method returns it doesn't mean that the packet was sent out.
  // but that it was merely queued up. Server now owns the response object
  void SendResponse(REQID id, Connection* connection, const google::protobuf::Message& response);

  // Send a message to initiate a non request-response style communication
  // message is now owned by the Server class
  void SendMessage(Connection* connection, const google::protobuf::Message& message);

  // Close a connection. This function doesn't return anything.
  // When the connection is attempted to be closed(which can happen
  // at a later time if using thread pool), The HandleConnectionClose
  // will contain a status of how the closing process went.
  void CloseConnection(Connection* connection);

  // Add a timer function to be called after msecs microseconds.
  void AddTimer(VCallback<> cb, sp_int64 msecs);

  // Register a handler for a particular request type
  template <typename T, typename M>
  void InstallRequestHandler(void (T::*method)(REQID id, Connection* conn, M*)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    requestHandlers[m->GetTypeName()] = std::bind(&Server::dispatchRequest<T, M>, this, t, method,
                                                  std::placeholders::_1, std::placeholders::_2);
    delete m;
  }

  // Register a handler for a particular message type
  template <typename T, typename M>
  void InstallMessageHandler(void (T::*method)(Connection* conn, M*)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    messageHandlers[m->GetTypeName()] = std::bind(&Server::dispatchMessage<T, M>, this, t, method,
                                                  std::placeholders::_1, std::placeholders::_2);
    delete m;
  }

  // One can also send requests to the client
  void SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                   google::protobuf::Message* _response_placeholder);
  void SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                   sp_int64 _msecs, google::protobuf::Message* _response_placeholder);

  // Backpressure handler
  virtual void StartBackPressureConnectionCb(Connection* connection);
  // Backpressure Reliever
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  // Return the underlying EventLoop.
  RDMAEventLoop* getEventLoop() { return eventLoop_; }

protected:
  // Called when a new connection is accepted.
  virtual void HandleNewConnection(Connection* newConnection) = 0;

  // Called when a connection is closed.
  // The connection object must not be used by the application after this call.
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode _status) = 0;

  // Handle the responses for any sent requests
  // We provide a basic handler that just deletes the response
  virtual void HandleResponse(google::protobuf::Message* _response, void* _ctx,
                              NetworkErrorCode _status);

public:
  // The interfaces implemented of the BaseServer

  // Create the connection
  BaseConnection* CreateConnection(RDMAConnection* endpoint, RDMAOptions* options,
                                   RDMAEventLoop* ss);

  // Called when connection is accepted
  virtual void HandleNewConnection_Base(BaseConnection* newConnection);

  // Called when the connection is closed
  virtual void HandleConnectionClose_Base(BaseConnection* connection, NetworkErrorCode _status);

private:
  // When a new packet arrives on the connection, this is invoked by the Connection
  void OnNewPacket(Connection* connection, IncomingPacket* packet);

  void InternalSendResponse(Connection* _connection, OutgoingPacket* _packet);

  template <typename T, typename M>
  void dispatchRequest(T* _t, void (T::*method)(REQID id, Connection* conn, M*), Connection* _conn,
                       IncomingPacket* _ipkt) {
    REQID rid;
    CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";
    M* m = new M();
    if (_ipkt->UnPackProtocolBuffer(m) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      delete m;
        CloseConnection(_conn);
      return;
    }
    CHECK(m->IsInitialized());

    std::function<void()> cb = std::bind(method, _t, rid, _conn, m);

    cb();
  }

  template <typename T, typename M>
  void dispatchMessage(T* _t, void (T::*method)(Connection* conn, M*), Connection* _conn,
                       IncomingPacket* _ipkt) {
    REQID rid;
    CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";
    M* m = new M();
    if (_ipkt->UnPackProtocolBuffer(m) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      delete m;
      CloseConnection(_conn);
      return;
    }
    CHECK(m->IsInitialized());

    std::function<void()> cb = std::bind(method, _t, _conn, m);

    cb();
  }

  void InternalSendRequest(Connection* _conn, google::protobuf::Message* _request, sp_int64 _msecs,
                           google::protobuf::Message* _response_placeholder, void* _ctx);
  void OnPacketTimer(REQID _id, RDMAEventLoop::Status status);

  typedef std::function<void(Connection*, IncomingPacket*)> handler;
  std::unordered_map<std::string, handler> requestHandlers;
  std::unordered_map<std::string, handler> messageHandlers;

  // For acting like a client
  std::unordered_map<REQID, std::pair<google::protobuf::Message*, void*>, REQID_Hash > context_map_;
  REQID_Generator* request_rid_gen_;
};

#endif  // SERVER_H_
