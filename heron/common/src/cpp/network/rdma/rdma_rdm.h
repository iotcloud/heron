#ifndef RDMA_RDM_H_
#define RDMA_RDM_H_

#include <cstdint>
#include <unistd.h>
#include <stdio.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>
#include <glog/logging.h>

#include "network/rdma/rdma_buffer.h"
#include "network/rdma/utils.h"
#include "network/rdma/options.h"
#include "network/rdma/rdma_event_loop.h"
#include "network/rdma/rdma_packet.h"
#include "network/rdma/rdma_channel.h"

/**
 * We use tag messagging
 * | 16                       | 16       | 16               | 16
 * | Type - control=0, data=1 | sub type | target_stream_id | stream_id
 */

class RDMADatagramChannel;

class RDMADatagram {
public:
  RDMADatagram(RDMAOptions *opts, RDMAFabric *fabric, uint16_t stream_id);
  void Free();

  virtual ~RDMADatagram();

  int start();

  struct fid_ep *GetEp() {
    return ep;
  }

  // disconnect
  int closeConnection();

  // remove end has done a connection close
  int ConnectionClosed();

  void Loop();

  /**
 * Allocate the resources for this connection
 */
  int SetupQueues();

  int PostBuffers();
  /**
   * Set and initialize the end point
   */
  int InitEndPoint();

  RDMADatagramChannel* CreateChannel(uint16_t target_id, struct fi_info *target);
  RDMADatagramChannel* GetChannel(uint16_t target_id);
  int SendAddressToRemote(fi_addr_t remote, uint16_t target_id);
  void AddChannel(uint16_t target_id, RDMADatagramChannel *channel);
  void SetRDMConnect(VCallback<uint32_t> connect) {
    this->onRDMConnect = connect;
  };

  int Wait();
  int Stop();

  /**
* Transmit a buffer
*/
  ssize_t PostTX(size_t size, int index, fi_addr_t addr, uint16_t target_id, uint16_t type);
  ssize_t PostTX(size_t size, int index, fi_addr_t addr, uint64_t tag);
  /**
   * Post a receive buffer
   */
  ssize_t PostRX(size_t size, int index);
private:
  // options for initialization
  RDMAOptions *options;
  uint16_t stream_id;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the fabric
  struct fid_fabric *fabric;
  // fabric domain we are working with
  struct fid_domain *domain;
  // end point
  struct fid_ep *ep, *alias_ep;
  // cq attribute for getting completion notifications
  struct fi_cq_attr cq_attr;
  // transfer cq and receive cq
  struct fid_cq *txcq, *rxcq;
  // receive cq fd and transmit cq fd
  int rx_fd, tx_fd;
  // buffer used for communication
  uint8_t *buf;
  uint8_t *w_buf;
  RDMABuffer *recv_buf;
  RDMABuffer *send_buf;

  struct fid_mr *mr;
  struct fid_mr *w_mr;

  // sequence numbers for messages posted
  uint64_t tx_seq;
  // completed transfer requests
  uint64_t tx_cq_cntr;
  // receives posted
  uint64_t rx_seq;
  // receive completed
  uint64_t rx_cq_cntr;

  // data structures for psm2
  struct fid_av *av;
  struct fi_av_attr av_attr;

  /** Private methods */
  int AllocateBuffers(void);
  int TransmitComplete();
  int ReceiveComplete();

  int AVInsert(void *addr, size_t count, fi_addr_t *fi_addr,
                             uint64_t flags, void *context);


  int HandleConnect(uint16_t connect_type, int bufer_index, uint16_t target_id);
  int SendConfirmToRemote(fi_addr_t remote);
  int CreditReadComplete(RDMADatagramChannel *channel, uint32_t count);
  int DataReadComplete(RDMADatagramChannel *channel, uint32_t count);
  int CreditWriteComplete(RDMADatagramChannel *channel, uint32_t count);
  int DataWriteComplete(RDMADatagramChannel *channel, uint32_t count);

  // remote rdm channels
  std::unordered_map<uint16_t, RDMADatagramChannel *> channels;
  // the tag used by control messages
  uint64_t recv_tag;
  // the control ignore bits
  uint64_t tag_mask;

  uint32_t connect_buffers;
  // array of io vectors
  struct iovec *io_vectors;
  // array of tag messages
  struct fi_msg_tagged *tag_messages;
  struct fi_context *recv_contexts;
  struct fi_context *tx_contexts;
  VCallback<uint32_t> onRDMConnect;
  VCallback<uint32_t> onRDMConfirm;

  pthread_t loopThreadId;
  bool run;
  uint32_t buffs_per_channel;
};

class RDMADatagramChannel : public RDMAChannel {
public:

  RDMADatagramChannel(RDMAOptions *opts,
                      uint16_t stream_id, uint16_t recv_stream_id,
                      fi_addr_t	remote_addr, RDMABuffer *recv,
                      RDMABuffer *send, RDMADatagram *dgram, uint32_t max_buffs);
  void Free();

  virtual ~RDMADatagramChannel();

  int start();

  int WriteData(uint8_t *buf, uint32_t size, uint32_t *writem);

  int ReadData(uint8_t *buf, uint32_t size, uint32_t *read);

  struct fid_ep *GetEp() {
    return ep;
  }

  // disconnect
  int closeConnection();

  // remove end has done a connection close
  int ConnectionClosed();

  uint32_t getPort();

  char *getIPAddress();

  fi_addr_t GetRemoteAddress() {
    return remote_addr;
  }

  int setOnWriteComplete(VCallback<uint32_t> onWriteComplete);

  int registerRead(VCallback<int> onWrite);
  int registerWrite(VCallback<int> onWrite);
  int DataWriteCompleted();
  int ReadReady(ssize_t cq_read);
  int WriteData();
  int PostCreditIfNeeded();
  int WriteData(RDMAOutgoingPacket *packet, uint32_t *write);
  int ReadData(RDMAIncomingPacket *packet, uint32_t *read);
  uint32 MaxWritableBufferSize();
  int setOnIncomingPacketPackReady(VCallback<RDMAIncomingPacket *> onIncomingPacketPack);

  int CreditWriteCompleted();
  int CreditReadComplete();
  int PostBufferAfterRead(int index, int used_credit);
  bool WriteReady();
private:
  // options for initialization
  RDMAOptions *options;
  uint16_t stream_id;
  uint16_t target_stream_id;
  RDMADatagram *datagram;
  // fabric information obtained
  struct fi_info *info;
  // the fabric
  // fabric domain we are working with
  struct fid_domain *domain;
  // end point
  struct fid_ep *ep;
  // cq attribute for getting completion notifications
  struct rdma_loop_info tx_loop, rx_loop;

  RDMABuffer *recv_buf;
  RDMABuffer *send_buf;

  int postCredit();

  VCallback<uint32_t> onWriteComplete;
  VCallback<int> onWriteReady;
  VCallback<int> onReadReady;
  VCallback<RDMAIncomingPacket *> onIncomingPacketPackReady;

  // the max credit this channel entitled to
  uint32_t max_buffers;
  uint32_t written_buffers;
  // number of messages received after last sent credit
  uint64_t total_used_credit;
  // at which total credit, we sent the credit to the other side
  uint64_t credit_used_checkpoint;
  // credit of the peer as we know it
  // when we transmit a message, we reduce the peer credit until
  // the peer notifies us with its new credit
  uint32_t peer_credit;
  // if a write is waiting for credit
  bool waiting_for_credit;

  // the remote address
  fi_addr_t	remote_addr;
  // the tag used by this communications
  uint64_t send_tag;
  uint64_t send_credit_tag;
  uint64_t tag_mask;
  bool started;
  Timer t;
};

#endif