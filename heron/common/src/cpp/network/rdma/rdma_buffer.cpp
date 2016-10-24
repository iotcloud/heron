#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cinttypes>
#include <cstring>
#include <glog/logging.h>

#include "hps.h"
#include "rdma_buffer.h"

RDMABuffer::RDMABuffer(uint8_t *buf, uint32_t total_buffer_size, uint32_t no_bufs) {
  this->buf = buf;
  this->buf_size = total_buffer_size / no_bufs;
  this->no_bufs = no_bufs;
  this->base = 0;
  this->current_read_index = 0;
  this->buffers = NULL; // do error handling
  this->submitted_buffs = 0;
  this->filled_buffs = 0;

  pthread_mutex_init(&lock, NULL);
  Init();
}

int RDMABuffer::acquireLock() {
  return 0;
//  return pthread_mutex_lock(&lock);
}

int RDMABuffer::releaseLock() {
  return 0;
//  return pthread_mutex_unlock(&lock);
}

uint8_t * RDMABuffer::GetBuffer(int i) {
  return buffers[i];
}

uint32_t RDMABuffer::GetBufferSize() {
  return buf_size;
};

uint32_t RDMABuffer::GetNoOfBuffers() {
  return no_bufs;
}

uint32_t RDMABuffer::GetBase() {
  return base;
}

uint32_t RDMABuffer::GetCurrentReadIndex() {
  return this->current_read_index;
}

int RDMABuffer::Init() {
  uint32_t i = 0;
  this->buffers = (uint8_t **)malloc(sizeof(uint8_t *) * no_bufs);
  for (i = 0; i < no_bufs; i++) {
    this->buffers[i] = this->buf + buf_size * i;
  }
  this->content_sizes = (uint32_t *)malloc(sizeof(uint32_t *) * no_bufs);
  this->base = 0;
  return 0;
}

int RDMABuffer::IncrementFilled(uint32_t count) {
  uint32_t temp = this->filled_buffs + count;
  if (temp > this->no_bufs) {
    LOG(ERROR) << "Failed to increment the submitted, inconsistant state";
    return 1;
  }
  this->filled_buffs = temp;
  return 0;
}

int RDMABuffer::IncrementSubmitted(uint32_t count) {
  uint32_t temp = this->submitted_buffs + count;
  if (temp > this->no_bufs) {
    LOG(ERROR) << "Failed to increment the submitted, inconsistent state";
    return 1;
  }
  this->submitted_buffs = temp;
  return 0;
}

int RDMABuffer::IncrementBase(uint32_t count) {
  if (this->filled_buffs - count < 0 || this->submitted_buffs - count < 0) {
    LOG(ERROR) << "Failed to decrement the buffer, inconsistent state";
    return 1;
  }
  this->base = (this->base + count) % this->no_bufs;
  // dec submitted and filled
  this->submitted_buffs -= count;
  this->filled_buffs -= count;
  return 0;
}

int RDMABuffer::setBufferContentSize(uint32_t index, uint32_t size) {
  if (index < 0 || index >= no_bufs) {
    LOG(ERROR) << "Index out of bound " << index;
    return 1;
  }
  this->content_sizes[index] = size;
  return 0;
}

uint32_t RDMABuffer::getContentSize(uint32_t index) {
  if (index < 0 || index >= no_bufs) {
    LOG(ERROR) << "Index out of bound " << index;
    return 1;
  }
  return this->content_sizes[index];
}

void RDMABuffer::Free() {
  if (this->buffers) {
    free(this->buffers);
  }
}

uint64_t RDMABuffer::GetAvailableWriteSpace() {
  // get the total free space available
  int free_slots = this->no_bufs - this->filled_buffs;
  return free_slots * this->buf_size;
}

uint32_t RDMABuffer::NextWriteIndex() {
  return (base + this->filled_buffs) % this->no_bufs;
}


