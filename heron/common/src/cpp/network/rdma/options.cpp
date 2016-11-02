#include <cstdio>
#include <cstdlib>

#include "network/rdma/options.h"
#include "network/rdma/utils.h"

RDMAOptions::RDMAOptions() {
  this->dst_addr = NULL;
  this->dst_port = NULL;
  this->src_addr = NULL;
  this->src_port = NULL;
  this->buf_size = 0;
  this->no_buffers = 4;
  this->max_packet_size_ = 1024;
}

void RDMAOptions::Free() {
  if (this->dst_addr) {
    free(this->dst_addr);
  }
  if (this->dst_port) {
    free(this->dst_port);
  }
  if (this->src_addr) {
    free(this->src_addr);
  }
}

void RDMAOptions::SetSource(char *src_addr, char *src_port) {
  this->src_addr = src_addr;
  this->src_port = src_port;
}

void RDMAOptions::SetDest(char *dst_addr, char *dst_port) {
  this->dst_addr = dst_addr;
  this->dst_port = dst_port;
}