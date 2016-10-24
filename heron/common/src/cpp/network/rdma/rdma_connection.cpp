#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <arpa/inet.h>
#include <glog/logging.h>

#include "rdma_connection.h"

#define HPS_EP_BIND(ep, fd, flags)					\
	do {								\
		int ret;						\
		if ((fd)) {						\
			ret = fi_ep_bind((ep), &(fd)->fid, (flags));	\
			if (ret) {					\
				HPS_ERR("fi_ep_bind %d", ret);		\
				return ret;				\
			}						\
		}							\
	} while (0)


RDMAConnection::RDMAConnection(RDMAOptions *opts, struct fi_info *info,
                       struct fid_fabric *fabric, struct fid_domain *domain, RDMAEventLoop *loop) {
  this->options = opts;
  this->info = info;
  this->info_hints = info_hints;
  this->fabric = fabric;
  this->domain = domain;
  this->mState = INIT;
  this->eventLoop = loop;

  this->txcq = NULL;
  this->rxcq = NULL;

  this->tx_loop.callback = [this](enum rdma_loop_status state) { return this->OnWrite(state); };
  this->rx_loop.callback = [this](enum rdma_loop_status state) { return this->OnRead(state); };
  this->tx_loop.event = CQ_TRANSMIT;
  this->rx_loop.event = CQ_READ;

  this->ep = NULL;
  this->alias_ep = NULL;
  this->mr = NULL;
  this->no_mr = {};

  this->rx_fd = 0;
  this->tx_fd = 0;

  this->buf = NULL;
  this->recv_buf = NULL;
  this->send_buf = NULL;

  this->cq_attr = {};
  this->tx_ctx = {};
  this->rx_ctx = {};

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;

  this->ft_skip_mr = 0;

  this->cq_attr.wait_obj = FI_WAIT_NONE;
  this->timeout = -1;

  this->self_credit = 0;
  this->total_sent_credit = 0;
  this->peer_credit = 0;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
}

void RDMAConnection::Free() {
  if (mr != &no_mr) {
    HPS_CLOSE_FID(mr);
  }
  HPS_CLOSE_FID(alias_ep);
  HPS_CLOSE_FID(ep);
  HPS_CLOSE_FID(rxcq);
  HPS_CLOSE_FID(txcq);

  if (buf) {
    free(buf);
  }

  if (recv_buf) {
    recv_buf->Free();
    delete recv_buf;
  }

  if (send_buf) {
    send_buf->Free();
    delete send_buf;
  }
}

int RDMAConnection::start() {
  LOG(INFO) << "Starting rdma connection";
  int ret = PostBuffers();
  if (ret) {
    HPS_ERR("Failed to set up the buffers %d", ret);
    return ret;
  }

  // registe with the loop
  ret = this->eventLoop->RegisterRead(&this->rxcq->fid, &rx_loop);
  if (ret) {
    HPS_ERR("Failed to register receive cq to event loop %d", ret);
    return ret;
  }

  ret = this->eventLoop->RegisterRead(&this->txcq->fid, &tx_loop);
  if (ret) {
    HPS_ERR("Failed to register transmit cq to event loop %d", ret);
    return ret;
  }

  mState = CONNECTED;
  return 0;
}

int RDMAConnection::registerWrite(VCallback<int> onWrite) {
  this->onWriteReady = std::move(onWrite);
  return 0;
}

int RDMAConnection::registerRead(VCallback<int> onRead) {
  this->onReadReady = std::move(onRead);
  return 0;
}

int RDMAConnection::setOnWriteComplete(VCallback<uint32_t> onWriteComplete) {
  this->onWriteComplete = std::move(onWriteComplete);
  return 0;
}

int RDMAConnection::SetupQueues() {
  int ret;
  ret = AllocateBuffers();
  if (ret) {
    return ret;
  }

  // we use the context, not the counter
  cq_attr.format = FI_CQ_FORMAT_CONTEXT;
  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_FD;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  cq_attr.size = info->tx_attr->size;
  ret = fi_cq_open(domain, &cq_attr, &txcq, &txcq);
  if (ret) {
    HPS_ERR("fi_cq_open for send %d", ret);
    return ret;
  }

  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_FD;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  cq_attr.size = info->rx_attr->size;
  ret = fi_cq_open(domain, &cq_attr, &rxcq, &rxcq);
  if (ret) {
    HPS_ERR("fi_cq_open for receive %d", ret);
    return ret;
  }

  return 0;
}

int RDMAConnection::AllocateBuffers(void) {
  int ret = 0;
  long alignment = 1;
  RDMAOptions *opts = this->options;
  uint8_t *tx_buf, *rx_buf;
  size_t buf_size, tx_size, rx_size;

  tx_size = opts->buf_size / 2;
  if (tx_size > info->ep_attr->max_msg_size) {
    tx_size = info->ep_attr->max_msg_size;
  }
  rx_size = tx_size;
  buf_size = tx_size + rx_size;

  if (opts->options & HPS_OPT_ALIGN) {
    alignment = sysconf(_SC_PAGESIZE);
    if (alignment < 0) {
      return -errno;
    }
    buf_size += alignment;

    ret = posix_memalign((void **)&buf, (size_t) alignment, buf_size);
    if (ret) {
      LOG(ERROR) << "Failed to align memory to a page " << ret;
      return ret;
    }
  } else {
    buf = (uint8_t *)malloc(buf_size);
    if (!buf) {
      LOG(ERROR) << "No memory in the system";
      return -FI_ENOMEM;
    }
  }
  memset(buf, 0, buf_size);
  rx_buf = buf;
  tx_buf = buf + rx_size;
  tx_buf = (uint8_t *) (((uintptr_t) tx_buf + alignment - 1) & ~(alignment - 1));

  if (!ft_skip_mr && ((info->mode & FI_LOCAL_MR) ||
                      (info->caps & (FI_RMA | FI_ATOMIC)))) {
    ret = fi_mr_reg(domain, buf, buf_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY, 0, &mr, NULL);
    if (ret) {
      LOG(ERROR) << "Failed to register memory: " << ret;
      return ret;
    }
  } else {
    LOG(ERROR) << "No memory";
    mr = &no_mr;
  }

  this->send_buf = new RDMABuffer(tx_buf, (uint32_t) tx_size, opts->no_buffers);
  this->recv_buf = new RDMABuffer(rx_buf, (uint32_t) rx_size, opts->no_buffers);
  return 0;
}

int RDMAConnection::InitEndPoint(struct fid_ep *ep, struct fid_eq *eq) {
  int ret;
  this->ep = ep;

  HPS_EP_BIND(ep, eq, 0);
  HPS_EP_BIND(ep, txcq, FI_TRANSMIT);
  HPS_EP_BIND(ep, rxcq, FI_RECV);

  ret = hps_utils_get_cq_fd(this->options, txcq, &tx_fd);
  if (ret) {
    HPS_ERR("Failed to get cq fd for transmission");
    return ret;
  }
  this->tx_loop.fid = tx_fd;

  ret = hps_utils_get_cq_fd(this->options, rxcq, &rx_fd);
  if (ret) {
    HPS_ERR("Failed to get cq fd for receive");
    return ret;
  }
  this->rx_loop.fid = rx_fd;

  ret = fi_enable(ep);
  if (ret) {
    LOG(ERROR) << "Failed to enable endpoint " << ret;
    return ret;
  }
  return 0;
}

int RDMAConnection::PostBuffers() {
  this->rx_seq = 0;
  this->rx_cq_cntr = 0;
  this->tx_cq_cntr = 0;
  this->tx_seq = 0;
  ssize_t ret = 0;
  RDMABuffer *rBuf = this->recv_buf;
  uint32_t noBufs = rBuf->GetNoOfBuffers();
  for (uint32_t i = 0; i < noBufs; i++) {
    uint8_t *buf = rBuf->GetBuffer(i);
    ret = PostRX(rBuf->GetBufferSize(), buf, &rx_ctx);
    if (ret) {
      LOG(ERROR) << "Error posting receive buffer" << ret;
      return (int) ret;
    }
    rBuf->IncrementSubmitted(1);
  }
  this->self_credit = rBuf->GetNoOfBuffers();
  this->peer_credit = rBuf->GetNoOfBuffers()  - 1;
  this->total_sent_credit = rBuf->GetNoOfBuffers() - 1;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->credit_messages_ = new bool[noBufs];
  memset(this->credit_messages_, 0, sizeof(bool) * noBufs);
  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
int RDMAConnection::SpinForCompletion(struct fid_cq *cq, uint64_t *cur,
                                  uint64_t total, int timeout) {
  struct fi_cq_err_entry comp;
  struct timespec a, b;
  ssize_t ret;

  if (timeout >= 0) {
    clock_gettime(CLOCK_MONOTONIC, &a);
  }

  while (total - *cur > 0) {
    LOG(ERROR) << "Spin for completion";
    ret = fi_cq_read(cq, &comp, 1);
    if (ret > 0) {
      if (timeout >= 0) {
        clock_gettime(CLOCK_MONOTONIC, &a);
      }

      (*cur)++;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return (int) ret;
    } else if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &b);
      if ((b.tv_sec - a.tv_sec) > timeout) {
        return -FI_ENODATA;
      }
    }
  }

  return 0;
}

int RDMAConnection::GetCQComp(struct fid_cq *cq, uint64_t *cur,
                          uint64_t total, int timeout) {
  int ret;
  ret = SpinForCompletion(cq, cur, total, timeout);

  if (ret) {
    if (ret == -FI_EAVAIL) {
      ret = hps_utils_cq_readerr(cq);
      (*cur)++;
    } else {
      LOG(ERROR) << "ft_get_cq_comp " << ret;
    }
  }
  return ret;
}

int RDMAConnection::GetRXComp(uint64_t total) {
  int ret;
  if (rxcq) {
    ret = GetCQComp(rxcq, &rx_cq_cntr, total, timeout);
  } else {
    LOG(ERROR) << "Trying to get a RX completion when no RX CQ or counter were opened";
    ret = -FI_EOTHER;
  }
  return ret;
}

int RDMAConnection::GetTXComp(uint64_t total) {
  int ret;

  if (txcq) {
    ret = GetCQComp(txcq, &tx_cq_cntr, total, -1);
  } else {
    LOG(ERROR) << "Trying to get a TX completion when no TX CQ or counter were opened";
    ret = -FI_EOTHER;
  }
  return ret;
}

ssize_t RDMAConnection::PostTX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  int timeout_save;
  ssize_t ret, rc;

  while (1) {
    ret = fi_send(this->ep, buf, size, fi_mr_desc(mr),	0, ctx);
    if (!ret)
      break;

    if (ret != -FI_EAGAIN) {
      LOG(ERROR) << "Error posting send " << ret;
      return ret;
    }

    timeout_save = timeout;
    timeout = 0;
    rc = GetTXComp(tx_cq_cntr + 1);
    if (rc && rc != -FI_EAGAIN) {
      LOG(ERROR) << "Failed to get completion for write";
      return rc;
    }
    LOG(INFO) << "Loooping for tx completion";
    timeout = timeout_save;
  }
  tx_seq++;
  return 0;
}

ssize_t RDMAConnection::PostRX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  int timeout_save;
  ssize_t ret, rc;

  while (1) {
    ret = fi_recv(this->ep, buf, size, fi_mr_desc(mr),	0, ctx);
    if (!ret)
      break;

    if (ret != -FI_EAGAIN) {
      LOG(ERROR) << "Error posting receive " << ret;
      return ret;
    }

    timeout_save = timeout;
    timeout = 0;
    rc = GetRXComp(rx_cq_cntr + 1);
    if (rc && rc != -FI_EAGAIN) {
      LOG(ERROR) << "Failed to get completion for receive";
      return rc;
    }
    LOG(INFO) << "Loooping for rx completion";
    timeout = timeout_save;
  }
  rx_seq++;
  return 0;
}

bool RDMAConnection::DataAvailableForRead() {
  RDMABuffer *sbuf = this->recv_buf;
  return sbuf->GetFilledBuffers() > 0;
}

int RDMAConnection::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  ssize_t ret = 0;
  uint32_t base;
  uint32_t submittedBuffers;
  uint32_t noOfBuffers;
  uint32_t index = 0;
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;
  // now lock the buffer
  rbuf->acquireLock();
  if (rbuf->GetFilledBuffers() == 0) {
    *read = 0;
    rbuf->releaseLock();
    return 0;
  }
  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  uint32_t current_read_indx = rbuf->GetCurrentReadIndex();
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
  while (read_size < size &&  buffers_filled > 0) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;
    int32_t *credit = (int32_t *) (b + sizeof(uint32_t));
    // update the peer credit with the latest
    if (*credit >= 0) {
      this->peer_credit += *credit;
      // lets mark it zero in case we are not moving to next buffer
      *credit = 0;
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
      credit_messages_[tail] = length <= 0;
      // advance the base pointer
      rbuf->IncrementBase(1);

      // this->self_credit--;
      buffers_filled--;
      tail = rbuf->GetBase();
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

  base = rbuf->GetBase();
  submittedBuffers = rbuf->GetSubmittedBuffers();
  noOfBuffers = rbuf->GetNoOfBuffers();
  while (submittedBuffers < noOfBuffers) {
    index = (base + submittedBuffers) % noOfBuffers;
    uint8_t *send_buf = rbuf->GetBuffer(index);
    ret = PostRX(rbuf->GetBufferSize(), send_buf, &this->rx_ctx);
    if (ret) {
      LOG(ERROR) << "Failed to post the receive buffer: " << ret;
      rbuf->releaseLock();
      return (int) ret;
    }
    this->total_used_credit++;
    rbuf->IncrementSubmitted(1);
    int32_t available_credit = total_used_credit - credit_used_checkpoint;
    if ((available_credit >= (noOfBuffers / 2 - 1)) && self_credit > 0) {
      if (available_credit > noOfBuffers - 1) {
        LOG(ERROR) << "Credit should never be greater than no of buffers available: "
                   << available_credit << " > " << noOfBuffers;
      }
      postCredit();
    }
//    LOG(ERROR) << "Read self: " << self_credit << " peer: " << peer_credit << " sent_credit: " << total_sent_credit << " used_credit: " << total_used_credit << " checkout: " << credit_used_checkpoint;
    submittedBuffers++;
  }
  rbuf->releaseLock();

  return 0;
}

int RDMAConnection::postCredit() {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t head = 0;
  uint32_t error_count = 0;
  sbuf->acquireLock();
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
    int32_t *sent_credit = (int32_t *) (current_buf + sizeof(uint32_t));
    int32_t available_credit = total_used_credit - credit_used_checkpoint;
    if (available_credit > sbuf->GetNoOfBuffers() - 1) {
      LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                 << available_credit << " > " << sbuf->GetNoOfBuffers();
      available_credit = sbuf->GetNoOfBuffers() - 1;
    }
    *sent_credit = available_credit;
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, 0);
    // send the current buffer
    if (!PostTX(sizeof(uint32_t) + sizeof(int32_t), current_buf, &this->tx_ctx)) {
      total_sent_credit += available_credit;
      credit_used_checkpoint += available_credit;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
//      LOG(ERROR) << "Post self: " << self_credit << " peer: "
//                 << peer_credit << " sent_credit: " << total_sent_credit << " used_credit: " << total_used_credit << " checkout: " << credit_used_checkpoint;
    } else {
      LOG(ERROR) <<  "Failed to transmit the buffer";
      error_count++;
      if (error_count > MAX_ERRORS) {
        LOG(ERROR) << "Failed to send the buffer completely. sent ";
        goto err;
      }
    }
  } else {
    LOG(ERROR) << "Free space not available to post credit "
               << " self: " << self_credit << " peer: " << peer_credit;
  }
  sbuf->releaseLock();

  return 0;

  err:
  sbuf->releaseLock();
  return 1;
}

int RDMAConnection::WriteData(uint8_t *buf, uint32_t size, uint32_t *write) {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;
  bool credit_set;
  int32_t no_buffers = sbuf->GetNoOfBuffers();
  uint32_t buf_size = sbuf->GetBufferSize() - 4;
  //LOG(INFO) << "Lock with peer credit: " << this->peer_credit;
  sbuf->acquireLock();
  // we need to send everything by using the buffers available
  uint64_t free_space = sbuf->GetAvailableWriteSpace();
  uint32_t free_buffs = sbuf->GetNoOfBuffers() - sbuf->GetFilledBuffers();
  while (sent_size < size && free_space > 0 && this->peer_credit > 0 && free_buffs > 1) {
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
    int32_t *sent_credit = (int32_t *) (current_buf + sizeof(uint32_t));
    int32_t available_credit = total_used_credit - credit_used_checkpoint;
    if (available_credit > 0  && self_credit > 0) {
      *sent_credit =  available_credit;
      credit_set = true;
    } else {
      *sent_credit = -1;
    }

    memcpy(current_buf + sizeof(uint32_t) + sizeof(int32_t), buf + sent_size, current_size);
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, current_size);
    // send the current buffer
    if (!PostTX(current_size + sizeof(uint32_t) + sizeof(int32_t), current_buf, &this->tx_ctx)) {
      if (credit_set) {
        total_sent_credit += available_credit;
        credit_used_checkpoint += available_credit;
      }

      sent_size += current_size;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
      this->peer_credit--;
//      LOG(ERROR) << "Write self: " << self_credit << " peer: " << peer_credit << " sent_credit: " << total_sent_credit << " used_credit: " << total_used_credit << " checkout: " << credit_used_checkpoint;
    } else {
      LOG(ERROR) <<  "Failed to transmit the buffer";
      error_count++;
      if (error_count > MAX_ERRORS) {
        LOG(ERROR) << "Failed to send the buffer completely. sent " << sent_size;
        goto err;
      }
    }
    free_space = sbuf->GetAvailableWriteSpace();
  }

  *write = sent_size;
  sbuf->releaseLock();
  return 0;

  err:
    sbuf->releaseLock();
    return 1;
}

int RDMAConnection::TransmitComplete() {
  struct fi_cq_err_entry comp;
  ssize_t cq_ret;
  uint32_t completed_bytes = 0;
  RDMABuffer *sbuf = this->send_buf;
  // lets get the number of completions
  size_t max_completions = tx_seq - tx_cq_cntr;
  // we can expect up to this
  LOG(INFO) << "Transmit complete begin";
  uint64_t free_space = sbuf->GetAvailableWriteSpace();

  if (free_space > 0) {
    onWriteReady(0);
  }

  cq_ret = fi_cq_read(txcq, &comp, max_completions);
  if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
    // HPS_INFO("transmit complete %ld", cq_ret);
    return 0;
  }

  //HPS_INFO("tansmit complete %ld", cq_ret);
  LOG(INFO) << "Lock";
  this->send_buf->acquireLock();
  if (cq_ret > 0) {
    this->tx_cq_cntr += cq_ret;
    for (int i = 0; i < cq_ret; i++) {
      uint32_t base = this->send_buf->GetBase();
      completed_bytes += this->send_buf->getContentSize(base);
      if (this->send_buf->IncrementBase((uint32_t) 1)) {
        LOG(ERROR) << "Failed to increment buffer data pointer";
        this->send_buf->releaseLock();
        return 1;
      }
    }
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      LOG(ERROR) << "Error receive " << cq_ret;
      cq_ret = hps_utils_cq_readerr(txcq);
      this->tx_cq_cntr++;
    } else {
      LOG(ERROR) << "Write completion queue error " << cq_ret;
      this->send_buf->releaseLock();
      return (int) cq_ret;
    }
  }
  if (onWriteComplete != NULL && completed_bytes > 0) {
    // call the calback with the completed bytes
    onWriteComplete(completed_bytes);
  }
  this->send_buf->releaseLock();
  return 0;
}

int RDMAConnection::ReceiveComplete() {
  ssize_t cq_ret;
  struct fi_cq_err_entry comp;
  RDMABuffer *sbuf = this->recv_buf;
  // lets get the number of completions
  size_t max_completions = rx_seq - rx_cq_cntr;
  uint64_t read_available = sbuf->GetFilledBuffers();
  LOG(INFO) << "Receive complete begin";
  // we can expect up to this
  cq_ret = fi_cq_read(rxcq, &comp, max_completions);
  if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
    if (read_available > 0) {
      onReadReady(0);
    }
    return 0;
  }

  this->recv_buf->acquireLock();
  if (cq_ret > 0) {
    this->rx_cq_cntr += cq_ret;
    if (this->recv_buf->IncrementFilled((uint32_t) cq_ret)) {
      LOG(ERROR) << "Failed to increment buffer data pointer";
      this->recv_buf->releaseLock();
      return 1;
    }
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      LOG(INFO) << "Error in receive completion" << cq_ret;
      cq_ret = hps_utils_cq_readerr(rxcq);
      this->rx_cq_cntr++;
    } else {
      LOG(ERROR) << "Receive completion queue error" << cq_ret;
      this->recv_buf->releaseLock();
      return (int) cq_ret;
    }
  }
  this->recv_buf->releaseLock();

  read_available = sbuf->GetFilledBuffers();
  if (read_available > 0) {
    onReadReady(0);
  }
  return 0;
}

RDMAConnection::~RDMAConnection() {}

void RDMAConnection::OnWrite(enum rdma_loop_status state) {
  TransmitComplete();
}

void RDMAConnection::OnRead(enum rdma_loop_status state) {
  ReceiveComplete();
}

int RDMAConnection::closeConnection() {
  if (mState != CONNECTED) {
    LOG(ERROR) << "Connection not in CONNECTED state, cannot disconnect";
  }

  if (eventLoop->UnRegister(rx_fd)) {
    LOG(ERROR) << "Failed to un-register read from loop";
  }

  if (eventLoop->UnRegister(tx_fd)) {
    LOG(ERROR) << "Failed to un-register transmit from loop";
  }

  int ret = fi_shutdown(ep, 0);
  if (ret) {
    LOG(ERROR) << "Failed to shutdown connection";
  }

  Free();
  mState = DISCONNECTED;
  return 0;
}

char* RDMAConnection::getIPAddress() {
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

uint32_t RDMAConnection::getPort() {
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


