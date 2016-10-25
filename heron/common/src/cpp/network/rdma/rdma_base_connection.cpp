#include <glog/logging.h>
#include "rdma_base_connection.h"

RDMABaseConnection::RDMABaseConnection(RDMAOptions *options, RDMAConnection *con,
                               RDMAEventLoopNoneFD *loop)
    : mRdmaConnection(con), mRdmaOptions(options), mEventLoop(loop){
  mState = INIT;
  mCanCloseConnection = true;
}

RDMABaseConnection::~RDMABaseConnection() { CHECK(mState == INIT || mState == DISCONNECTED); }

int32_t RDMABaseConnection::start() {
  if (mState != INIT) {
    LOG(ERROR) << "Connection not in INIT State, hence cannot start: " << mState;
    return -1;
  }

  mOnWrite = [this](int fd) { return this->handleWrite(fd); };
  mOnRead = [this](int fd) { return this->handleRead(fd); };

  mRdmaConnection->registerRead(mOnRead);
  mRdmaConnection->registerWrite(mOnWrite);

  if (mRdmaConnection->start()) {
    LOG(ERROR) << "Could not start the rdma connection";
    return -1;
  }
  LOG(INFO) << "Connection started";
  mState = CONNECTED;
  return 0;
}

void RDMABaseConnection::closeConnection() {
  if (mState != CONNECTED && mState != TO_BE_DISCONNECTED) {
    // Nothing to do here
    LOG(ERROR) << "Connection already closed, hence doing nothing";
    return;
  }
  mState = TO_BE_DISCONNECTED;
  internalClose();
}

void RDMABaseConnection::internalClose() {
  if (mState != TO_BE_DISCONNECTED) return;
  //if (!mCanCloseConnection) return;
  mState = DISCONNECTED;

  // for now lets close
  // todo we need to have a good look
  mRdmaConnection->closeConnection();
}

std::string RDMABaseConnection::getIPAddress() {
  std::string addr_result = "";
  if (mState != CONNECTED) {
    LOG(ERROR) << "Not in connected state, cannot get port";
    return addr_result;
  }
  char *address = mRdmaConnection->getIPAddress();
  addr_result = string(address);
  return addr_result;
}

int32_t RDMABaseConnection::getPort() {
  if (mState != CONNECTED) {
    LOG(ERROR) << "Not in connected state, cannot get port";
    return -1;
  }
  return mRdmaConnection->getPort();
}

void RDMABaseConnection::registerForClose(VCallback<NetworkErrorCode> cb) {
  mOnClose = std::move(cb);
}

int RDMABaseConnection::readData(uint8_t *buf, uint32_t size, uint32_t *read) {
  return mRdmaConnection->ReadData(buf, size, read);
}

int RDMABaseConnection::writeData(uint8_t *buf, uint32_t size, uint32_t *write) {
  return mRdmaConnection->WriteData(buf, size, write);
}

// Note that we hold the mutex when we come to this function
int RDMABaseConnection::handleWrite(int fd) {
  if (mState != CONNECTED) {
    // LOG(ERROR) << "Not connected";
    mState = TO_BE_DISCONNECTED;
    internalClose();
    return 0;
  }

  int32_t writeStatus = writeIntoEndPoint(fd);
  if (writeStatus < 0) {
    LOG(ERROR) << "Write failed, mark connection to be disconnected";
    mState = TO_BE_DISCONNECTED;
  }
  if (mState == CONNECTED && mWriteState == NOTREGISTERED && stillHaveDataToWrite()) {
    mWriteState = NOTREADY;
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
  return 0;
}

int RDMABaseConnection::handleRead(int fd) {
  if (mState != CONNECTED) {
    //LOG(ERROR) << "Not connected";
    mState = TO_BE_DISCONNECTED;
    internalClose();
    return 0;
  }
  LOG(INFO) << "Handler read";
  mReadState = READY;
  int32_t readStatus = readFromEndPoint(fd);
  if (readStatus >= 0) {
    mReadState = NOTREADY;
  } else {
    mReadState = ERROR;
    LOG(ERROR) << "Read failed, mark connection to be disconnected";
    mState = TO_BE_DISCONNECTED;
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  handleDataRead();
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
  return 0;
}

