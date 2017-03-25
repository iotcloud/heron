#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <arpa/inet.h>
#include <glog/logging.h>
#include "network/rdma/rdma_rdm.h"


#define HPS_EP_BIND(ep, fd, flags)					\
	do {								\
		int ret;						\
		if ((fd)) {						\
			ret = fi_ep_bind((ep), &(fd)->fid, (flags));	\
			if (ret) {					\
				LOG(ERROR) << "fi_ep_bind " << ret;		\
				return ret;				\
			}						\
		}							\
	} while (0)


RDMADatagramChannel::RDMADatagramChannel(RDMAOptions *opts, uint16_t stream_id, uint16_t recv_stream_id,
                                         fi_addr_t remote_addr, RDMABuffer *recv, RDMABuffer *send,
                                         RDMADatagram *dgram, uint32_t max_buffs) {
  this->options = opts;
  this->recv_buf = recv;
  this->send_buf = send;
  this->datagram = dgram;

  this->peer_credit = 0;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->stream_id = stream_id;
  this->target_stream_id = recv_stream_id;
  this->remote_addr = remote_addr;
  this->started = false;
  this->written_buffers = 0;
  this->max_buffers = max_buffs;
  this->onIncomingPacketPackReady = NULL;
}

RDMADatagramChannel::~RDMADatagramChannel() {

}

void RDMADatagramChannel::Free() {

}

void RDMADatagramChannel::SetIDs(std::string _our_id, std::string _other_id) {

}

int RDMADatagramChannel::registerWrite(VCallback<int> onWrite) {
  this->onWriteReady = std::move(onWrite);
  return 0;
}

int RDMADatagramChannel::registerRead(VCallback<int> onRead) {
  this->onReadReady = std::move(onRead);
  return 0;
}

int RDMADatagramChannel::setOnWriteComplete(VCallback<uint32_t> onWriteComplete) {
  this->onWriteComplete = std::move(onWriteComplete);
  return 0;
}

int RDMADatagramChannel::start() {
  if (started) {
    return 0;
  }

  uint16_t message_type = 1;
  uint16_t credit_type = 1;
  send_tag = 0;
  // create the receive tag and send tag
  send_tag = (uint64_t)stream_id << 32 | (uint64_t)target_stream_id << 48 | (uint64_t) message_type;
  send_credit_tag = (uint64_t)stream_id << 32 | (uint64_t)target_stream_id << 48 | (uint64_t) message_type | (uint64_t) credit_type << 16;

  uint64_t mask = 0;
//  for (int i = 0; i < 16; i++) {
//    mask = mask | ((uint64_t)1 << i);
//  }
  tag_mask = ~mask;
  LOG(INFO) << "Mask of stream id: " << stream_id << " target_id: " << target_stream_id << " mask: " << tag_mask;

  this->peer_credit = max_buffers - 2;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->waiting_for_credit = false;

  started = true;
  return 0;
}

int RDMADatagramChannel::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;
  // now lock the buffer
  if (rbuf->GetFilledBuffers() == 0) {
    *read = 0;
    return 0;
  }
  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  uint32_t current_read_indx = rbuf->GetCurrentReadIndex();
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
  // LOG(INFO) << "Buffs filled: " << buffers_filled;
  if (read_size < size &&  buffers_filled == 1) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;
    uint32_t *credit = (uint32_t *) (b + sizeof(uint32_t));
    // update the peer credit with the latest
    if (*credit > 0) {
//      LOG(INFO) << "Incrementing peer credit: " << peer_credit << " by: " << *credit;
      this->peer_credit += *credit;
      if (this->peer_credit > max_buffers - 2) {
        this->peer_credit = max_buffers - 2;
      }
      if (waiting_for_credit) {
        onWriteReady(0);
      }
      // LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit << " index: " << tail;
      // lets mark it zero in case we are not moving to next buffer
      *credit = 0;
    } else {
      // LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit << " index: " << tail;;
    }
    // now lets see how much data we need to copy from this buffer
    need_copy = (*length) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
      can_copy = need_copy;
      current_read_indx = 0;
      // LOG(INFO) << "Incrementing base";
      // advance the base pointer
      rbuf->IncrementBase(1);
    } else {
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    rbuf->setCurrentReadIndex(current_read_indx);
    // next copy the buffer
    memcpy(buf + read_size, b + sizeof(uint32_t) + sizeof(uint32_t) + tmp_index, can_copy);
    // now update
    read_size += can_copy;
  }
  *read = read_size;

  return 0;
}

uint32 RDMADatagramChannel::MaxWritableBufferSize() {
  return send_buf->GetBufferSize() - 8;
}

int RDMADatagramChannel::ReadData(RDMAIncomingPacket *packet, uint32_t *read) {
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;
  uint32_t size = packet->GetPacketSize();
  // now lock the buffer
  if (rbuf->GetFilledBuffers() == 0) {
    *read = 0;
    return 0;
  }
  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  uint32_t current_read_indx = rbuf->GetCurrentReadIndex();
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
  // LOG(INFO) << "Buffs filled: " << buffers_filled;
  if (buffers_filled == 1) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;
    uint32_t *credit = (uint32_t *) (b + sizeof(uint32_t));
    // update the peer credit with the latest
    if (*credit > 0) {
//      LOG(INFO) << "Incrementing peer credit: " << peer_credit << " by: " << *credit;
      this->peer_credit += *credit;
      if (this->peer_credit > max_buffers - 2) {
        this->peer_credit = max_buffers - 2;
      }
      if (waiting_for_credit) {
        onWriteReady(0);
      }
      // LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit << " index: " << tail;
      // lets mark it zero in case we are not moving to next buffer
      *credit = 0;
    } else {
      // LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit << " index: " << tail;;
    }
    // now lets see how much data we need to copy from this buffer
    need_copy = (*length) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
      can_copy = need_copy;
      current_read_indx = 0;
      // LOG(INFO) << "Incrementing base";
      // advance the base pointer
      rbuf->IncrementBase(1);
    } else {
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    rbuf->setCurrentReadIndex(current_read_indx);
    // next copy the buffer
    packet->SetBuffer((char *) (b + sizeof(uint32_t) + sizeof(uint32_t) + tmp_index));
    if (onIncomingPacketPackReady) {
      onIncomingPacketPackReady(packet);
    }
    // now update
    read_size += can_copy;
  }
  *read = read_size;

  return 0;
}

int RDMADatagramChannel::WriteData(RDMAOutgoingPacket *packet, uint32_t *write) {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  uint32_t size = packet->TotalSize();
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;
  bool credit_set;
  uint32_t buf_size = sbuf->GetBufferSize() - 8;
  // we need to send everything by using the buffers available
  uint32_t free_buffs = max_buffers - written_buffers;
  // LOG(ERROR) << "P:" << peer_credit << " max_buff: " << max_buffers << " written_buff:" << written_buffers;
  if (sent_size < size && this->peer_credit > 0 && free_buffs > 2) {
    credit_set = false;
    // we have space in the buffers
    head = sbuf->NextWriteIndex();
    uint8_t *current_buf = sbuf->GetBuffer(head);
    // now lets copy from send buffer to current buffer chosen
    current_size = (size - sent_size) < buf_size ? size - sent_size : buf_size;
    uint32_t *length = (uint32_t *) current_buf;
    // set the first 4 bytes as the content length
    *length = current_size;
    // send the credit with the write
    uint32_t *sent_credit = (uint32_t *) (current_buf + sizeof(uint32_t));
    uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
    if (available_credit > 0) {
      if (available_credit > max_buffers - 2) {
        LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                   << available_credit << " > " << max_buffers;
        available_credit = max_buffers - 2;
      }
      *sent_credit =  available_credit;
      credit_set = true;
    } else {
      *sent_credit = 0;
    }
    // LOG(INFO) << "Write credit: " << *sent_credit << " tuc: " << total_used_credit << " cuc: " << credit_used_checkpoint << " peer: " << peer_credit << " filled: " << sbuf->GetFilledBuffers() << " index: " << head;

    packet->Pack((char *) (current_buf + sizeof(uint32_t) + sizeof(int32_t)));
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, current_size);
    // send the current buffer
    ssize_t ret = datagram->PostTX(current_size + sizeof(uint32_t) + sizeof(int32_t), head, remote_addr, send_tag);
    if (!ret) {
      if (credit_set) {
        // LOG(INFO) << "Increment CUC: " << credit_used_checkpoint << " by " << available_credit;
        credit_used_checkpoint += available_credit;
      }
      written_buffers++;
      sent_size += current_size;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
      this->peer_credit--;
    } else {
      if (ret != -FI_EAGAIN) {
        LOG(ERROR) << "Failed to transmit the buffer";
        error_count++;
        if (error_count > MAX_ERRORS) {
          LOG(ERROR) << "Failed to send the buffer completely. sent " << sent_size;
          goto err;
        }
      }
    }
    waiting_for_credit = false;
  }
  if (peer_credit <= 0 && sent_size < size) {
    waiting_for_credit = true;
  }

  *write = sent_size;
  return 0;

  err:
  return -1;
}

int RDMADatagramChannel::setOnIncomingPacketPackReady(VCallback<RDMAIncomingPacket *> onIncomingPacketPack) {
  this->onIncomingPacketPackReady = onIncomingPacketPack;
  return 0;
}

int RDMADatagramChannel::PostBufferAfterRead(int index, int used_credit) {
  // LOG(INFO) << "Posting buffer index: " << index;
  ssize_t ret = datagram->PostRX(this->recv_buf->GetBufferSize(), index);
  if (ret) {
    LOG(ERROR) << "Failed to post buffer: " << ret;
    return -1;
  }
  this->total_used_credit += used_credit;
  return 0;
}

int RDMADatagramChannel::postCredit() {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t head = 0;
  uint32_t error_count = 0;
  // we need to send everything by using the buffers available
  uint64_t free_space = sbuf->GetAvailableWriteSpace();
  if (free_space > 0) {
    // we have space in the buffers
    head = sbuf->NextWriteIndex();
    uint8_t *current_buf = sbuf->GetBuffer(head);
    // now lets copy from send buffer to current buffer chosen
    uint32_t *length = (uint32_t *) current_buf;
    // set the first 4 bytes as the content length
    *length = 0;
    // send the credit with the write
    uint32_t *sent_credit = (uint32_t *) (current_buf + sizeof(uint32_t));
    uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
    if (available_credit > max_buffers - 2) {
      LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                 << available_credit << " > " << max_buffers;
      available_credit = max_buffers - 2;
    }
    *sent_credit = available_credit;
    // LOG(INFO) << "Posting credit: " << available_credit << " filled: " << sbuf->GetFilledBuffers() << " index: " << head;
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, 0);
    // send the current buffer
    ssize_t ret = datagram->PostTX(sizeof(uint32_t) + sizeof(int32_t), head, remote_addr, send_credit_tag);
    if (!ret) {
      written_buffers++;
       // LOG(INFO) << "Increment CUC: " << credit_used_checkpoint << " by " << available_credit;
      credit_used_checkpoint += available_credit;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
    } else {
      if (ret != -FI_EAGAIN) {
        LOG(ERROR) << "Failed to transmit the buffer";
        error_count++;
        if (error_count > MAX_ERRORS) {
          LOG(ERROR) << "Failed to send the buffer completely. sent ";
          goto err;
        }
      }
    }
  } else {
    LOG(ERROR) << "Free space not available to post credit "
               << " peer: " << peer_credit << "tuc: " << total_used_credit << " cuc: " << credit_used_checkpoint;
  }

  return 0;

  err:
  return 1;
}

int RDMADatagramChannel::WriteData(uint8_t *buf, uint32_t size, uint32_t *write) {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;
  bool credit_set;
  uint32_t buf_size = sbuf->GetBufferSize() - 8;
  // we need to send everything by using the buffers available
  uint32_t free_buffs = max_buffers - written_buffers;
  // LOG(ERROR) << "P:" << peer_credit << " max_buff: " << max_buffers << " written_buff:" << written_buffers;
  if (sent_size < size && this->peer_credit > 0 && free_buffs > 2) {
    credit_set = false;
    // we have space in the buffers
    head = sbuf->NextWriteIndex();
    uint8_t *current_buf = sbuf->GetBuffer(head);
    // now lets copy from send buffer to current buffer chosen
    current_size = (size - sent_size) < buf_size ? size - sent_size : buf_size;
    uint32_t *length = (uint32_t *) current_buf;
    // set the first 4 bytes as the content length
    *length = current_size;
    // send the credit with the write
    uint32_t *sent_credit = (uint32_t *) (current_buf + sizeof(uint32_t));
    uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
    if (available_credit > 0) {
      if (available_credit > max_buffers - 2) {
        LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                   << available_credit << " > " << max_buffers;
        available_credit = max_buffers - 2;
      }
      *sent_credit =  available_credit;
      credit_set = true;
    } else {
      *sent_credit = 0;
    }
    // LOG(INFO) << "Write credit: " << *sent_credit << " tuc: " << total_used_credit << " cuc: " << credit_used_checkpoint << " peer: " << peer_credit << " filled: " << sbuf->GetFilledBuffers() << " index: " << head;

    memcpy(current_buf + sizeof(uint32_t) + sizeof(int32_t), buf + sent_size, current_size);
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, current_size);
    // send the current buffer
    ssize_t ret = datagram->PostTX(current_size + sizeof(uint32_t) + sizeof(int32_t), head, remote_addr, send_tag);
    if (!ret) {
      if (credit_set) {
        // LOG(INFO) << "Increment CUC: " << credit_used_checkpoint << " by " << available_credit;
        credit_used_checkpoint += available_credit;
      }
      written_buffers++;
      sent_size += current_size;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
      this->peer_credit--;
    } else {
      if (ret != -FI_EAGAIN) {
        LOG(ERROR) << "Failed to transmit the buffer";
        error_count++;
        if (error_count > MAX_ERRORS) {
          LOG(ERROR) << "Failed to send the buffer completely. sent " << sent_size;
          goto err;
        }
      }
    }
    waiting_for_credit = false;
  }
  if (peer_credit <= 0 && sent_size < size) {
    waiting_for_credit = true;
  }

  *write = sent_size;
  return 0;

  err:
  return -1;
}

int RDMADatagramChannel::ReadReady(ssize_t cq_count){
  if (onReadReady != NULL) {
    if (recv_buf->GetFilledBuffers() > 0) {
      onReadReady(0);
    }
  } else {
     LOG(ERROR) << "Calling read without setting the callback function";
    return -1;
  }
  return 0;
}


bool RDMADatagramChannel::WriteReady() {
  return (max_buffers - written_buffers) > 2 && this->peer_credit > 0;
}

int RDMADatagramChannel::WriteData() {
  if (onWriteReady != NULL) {
    onWriteReady(0);
  } else {
    LOG(ERROR) << "Calling write without setting the callback function";
    return -1;
  }
  return 0;
}

int RDMADatagramChannel::DataWriteCompleted() {
  uint32_t completed_bytes = 0;
  uint32_t base = this->send_buf->GetBase();
  completed_bytes += this->send_buf->getContentSize(base);

  if (this->send_buf->IncrementBase((uint32_t) 1)) {
    LOG(ERROR) << "Failed to increment buffer data pointer";
    return 1;
  }

  written_buffers--;

  if (onWriteReady != NULL) {
    if (onWriteComplete != NULL && completed_bytes > 0) {
      // call the calback with the completed bytes
//      printf("Calling on write complete with: %d\n",  completed_bytes);
      onWriteComplete(completed_bytes);
    } else {
//      printf("onWriteComplete set: %d completed %d", (onWriteComplete != NULL), completed_bytes);
    }
    onWriteReady(0);
  } else {
    LOG(ERROR) << "Calling write without setting the callback function";
    return -1;
  }
  return 0;
}

int RDMADatagramChannel::CreditWriteCompleted() {
  if (this->send_buf->IncrementBase((uint32_t) 1)) {
    LOG(ERROR) << "Failed to increment buffer data pointer";
    return 1;
  }

  written_buffers--;

  if (onWriteReady != NULL) {
    onWriteReady(0);
  } else {
    LOG(ERROR) << "Calling write without setting the callback function";
    return -1;
  }
  return 0;

}

int RDMADatagramChannel::PostCreditIfNeeded() {
  uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
  // LOG(INFO) << "Post credit ReadData: " << total_used_credit << ", " << credit_used_checkpoint;
  if ((available_credit >= (max_buffers / 2 - 1))) {
    if (available_credit > max_buffers - 2) {
      LOG(ERROR) << "Credit should never be greater than no of buffers available: "
                 << available_credit << " > " << max_buffers;
    }
    postCredit();
  }
  return 0;
}

int RDMADatagramChannel::CreditReadComplete(){
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;

  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  // need to copy
  if (buffers_filled == 1) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;

    if (*length != 0) {
      LOG(ERROR) << "Credit message with length != 0";
      return 0;
    }

    uint32_t *credit = (uint32_t *) (b + sizeof(uint32_t));
    // LOG(INFO) << "Credit Received message with credit: " << *credit << " peer credit: " << peer_credit;
    // update the peer credit with the latest
    if (*credit > 0) {
      // LOG(INFO) << "Incrementing peer credit: " << peer_credit << " by: " << *credit;
      this->peer_credit += *credit;
      if (this->peer_credit > max_buffers - 2) {
        LOG(WARNING) << "Peer credit greater than number of buffers adjusting";
        this->peer_credit = max_buffers - 2;
      }
      if (waiting_for_credit) {
        onWriteReady(0);
      }
    }
    // advance the base pointer
    rbuf->IncrementBase(1);
  } else {
    LOG(WARNING) << "Un-expected no of buffers filled: " << buffers_filled;
  }

  return 0;
}

int RDMADatagramChannel::ConnectionClosed() {
  Free();
  return 0;
}

int RDMADatagramChannel::closeConnection() {
  Free();
  return 0;
}

char* RDMADatagramChannel::getIPAddress() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      LOG(ERROR) << "FI_ETOOSMALL, we shouln't get this";
    } else {
      LOG(ERROR) << "Failed to get peer address";
    }
  }

  char *addr_str = new char[INET_ADDRSTRLEN];
  struct sockaddr_in* addr_in = (struct sockaddr_in*)(&addr);
  inet_ntop(addr_in->sin_family, &(addr_in->sin_addr), addr_str, INET_ADDRSTRLEN);
  return addr_str;
}

uint32_t RDMADatagramChannel::getPort() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      LOG(ERROR) << "FI_ETOOSMALL, we shouln't get this";
    } else {
      LOG(ERROR) << "Failed to get peer address";
    }
  }
  uint32_t port = ntohs(((struct sockaddr_in*)(&addr))->sin_port);
  return port;
}
