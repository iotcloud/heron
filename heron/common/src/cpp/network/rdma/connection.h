#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "rdma_base_connection.h"
#include "packet.h"

class HeronRDMAConnection : public RDMABaseConnection {
public:

  HeronRDMAConnection(RDMAOptions *options, RDMAConnection *con, RDMAEventLoopNoneFD *loop);

  /**
   * `endpoint` is created by the caller, but now the Connection owns it.
   * `options` is also created by the caller and the caller owns it. options
   *  should be active throught the lifetime of the Connection object.
   */
  virtual ~HeronRDMAConnection();

  /**
   * Add this packet to the list of packets to be sent. The packet in itself can be sent
   * later. A zero return value indicates that the packet has been successfully queued to be
   * sent. It does not indicate that the packet was sent successfully. When the packet is
   * actually sent(or determined that it cannot be sent), the callback cb will be called
   * with appropriate status message.
   * A negative value of sendPacket indicates an error. The most likely error is improperly
   * formatted packet.
   * packet should not be touched by the caller until the callback cb has been called.
   */
  int32_t sendPacket(RDMAOutgoingPacket* packet, VCallback<NetworkErrorCode> cb);

  /**
   * This is the same as above except that we dont need any callback to confirm packet
   * delivery status.
   * packet is owned by the Connection object. It will be deleted after the packet has been
   * written down the wire.
   */
  int32_t sendPacket(RDMAOutgoingPacket* packet);

  /**
   * Invoke the callback cb when a new packet arrives. A pointer to the packet is passed
   * to the callback cb. That packet is now owned by the callback and is responsible for
   * deleting it.
   */
  void registerForNewPacket(VCallback<RDMAIncomingPacket*> cb);

  /**
   * The back pressure starter and reliever are used to communicate to the
   * server whether this connection is under a queue build up or not
   */
  int32_t registerForBackPressure(VCallback<HeronRDMAConnection*> cbStarter,
                                   VCallback<HeronRDMAConnection*> cbReliever);

  int32_t getOutstandingPackets() const { return mNumOutstandingPackets; }
  int32_t getOutstandingBytes() const { return mNumOutstandingBytes; }

  int32_t getWriteBatchSize() const { return mWriteBatchsize; }
  void setCausedBackPressure() { mCausedBackPressure = true; }
  void unsetCausedBackPressure() { mCausedBackPressure = false; }
  bool hasCausedBackPressure() const { return mCausedBackPressure; }
  bool isUnderBackPressure() const { return mUnderBackPressure; }

  int32_t putBackPressure();
  int32_t removeBackPressure();

public:
  // This is the high water mark on the num of bytes that can be left outstanding on a connection
  static int64_t systemHWMOutstandingBytes;
  // This is the low water mark on the num of bytes that can be left outstanding on a connection
  static int64_t systemLWMOutstandingBytes;

private:
  virtual int32_t writeIntoEndPoint(int fd);

  int writeComplete(ssize_t numWritten);

  virtual bool stillHaveDataToWrite();

  virtual int32_t readFromEndPoint(int fd);

  virtual void handleDataRead();

  int32_t ReadPacket();

  int32_t InternalPacketRead(char* _buffer, uint32_t _size, uint32_t *position_);

  // The list of outstanding packets that need to be sent.
  std::list<std::pair<RDMAOutgoingPacket*, VCallback<NetworkErrorCode>>> mOutstandingPackets;
  int32_t mNumOutstandingPackets;  // primarily because list's size is linear
  int32_t mNumOutstandingBytes;
  // number of packets we have written, but waiting for completion of the write
  sp_int32 mPendingWritePackets;

  // The list of packets that have been sent but not yet been reported to the higher layer
  std::list<std::pair<RDMAOutgoingPacket*, VCallback<NetworkErrorCode>>> mSentPackets;

  // The list of packets that have been received but not yet delivered to the higher layer
  std::list<RDMAIncomingPacket*> mReceivedPackets;

  // Incompletely read next packet
  RDMAIncomingPacket* mIncomingPacket;

  // The user registered callbacks
  VCallback<RDMAIncomingPacket*> mOnNewPacket;
  // This call back gets registered from the Server and gets called once the conneciton pipe
  // becomes free (outstanding bytes go to 0)
  VCallback<HeronRDMAConnection*> mOnConnectionBufferEmpty;
  // This call back gets registered from the Server and gets called once the conneciton pipe
  // becomes full (outstanding bytes exceed threshold)
  VCallback<HeronRDMAConnection*> mOnConnectionBufferFull;

  // How many bytes do we want to write in one batch
  uint32_t mWriteBatchsize;
  // Have we caused back pressure?
  bool mCausedBackPressure;
  // Are our reads being throttled?
  bool mUnderBackPressure;
  // How many times have we enqueued data and found that we had outstanding bytes >
  // HWM of back pressure threshold
  uint8_t mNumEnqueuesWithBufferFull;

  // the thread lock
  pthread_mutex_t lock;
};

#endif