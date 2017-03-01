#ifndef RDMA_CHANNEL_H_
#define RDMA_CHANNEL_H_

#include "network/rdma/rdma_fabric.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/rdma_packet.h"

class RDMAChannel {
public:
  virtual int start() = 0;
  virtual int ReadData(uint8_t *buf, uint32_t size, uint32_t *read) = 0;
  virtual int ReadData(RDMAIncomingPacket *packet, uint32_t *read) = 0;
  virtual int WriteData(uint8_t *buf, uint32_t size, uint32_t *write) = 0;
  virtual int WriteData(RDMAOutgoingPacket *packet, uint32_t *write) = 0;
  virtual uint32 MaxWritableBufferSize() = 0;
  virtual char *getIPAddress() = 0;
  virtual uint32_t getPort() = 0;
  virtual int registerRead(VCallback<int> onRead) = 0;
  virtual int registerWrite(VCallback<int> onWrite) = 0;
  virtual int closeConnection() = 0;
  virtual int ConnectionClosed() = 0;
  virtual int setOnWriteComplete(VCallback<uint32_t> onWriteComplete) = 0;
  virtual int setOnIncomingPacketPackReady(VCallback<RDMAIncomingPacket *> onIncomingPacketPack) = 0;
};

#endif