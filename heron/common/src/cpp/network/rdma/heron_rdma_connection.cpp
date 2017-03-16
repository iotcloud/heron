#include <glog/logging.h>
#include "heron_rdma_connection.h"
#include "network/rdma/rdma_rdm.h"

#define __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__ 1048576

const sp_int32 __SYSTEM_NETWORK_READ_BATCH_SIZE__ = 10485760;           // 1M
const sp_int32 __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__ = 10485760;  // 1M

// This is the high water mark on the num of bytes that can be left outstanding on a connection
sp_int64 HeronRDMAConnection::systemHWMOutstandingBytes = 1024 * 1024 * 100;  // 100M
// This is the low water mark on the num of bytes that can be left outstanding on a connection
sp_int64 HeronRDMAConnection::systemLWMOutstandingBytes = 1024 * 1024 * 50;  // 50M

HeronRDMAConnection::HeronRDMAConnection(RDMAOptions *options, RDMAChannel *con,
                                         RDMAEventLoop *loop)
    : RDMABaseConnection(options, con, loop),
      mNumOutstandingPackets(0),
      mNumOutstandingBytes(0)
    , mNumPendingWritePackets(0) {
  this->mRdmaConnection->setOnWriteComplete([this](uint32_t complets) {
    return this->writeComplete(complets); });
  this->mWriteBatchsize = __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__;
  mIncomingPacket = new RDMAIncomingPacket(1024*1024*10);
  mCausedBackPressure = false;
  mOnIncomingPacketBuild = NULL;
  pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
}

HeronRDMAConnection::HeronRDMAConnection(RDMAOptions *options, RDMAChannel *con,
                                         RDMAEventLoop *loop, ChannelType type)
    : RDMABaseConnection(options, con, loop, type),
      mNumOutstandingPackets(0),
      mNumOutstandingBytes(0)
    , mNumPendingWritePackets(0) {
  if (type == WRITE_ONLY || type == READ_WRITE) {
    this->mRdmaConnection->setOnWriteComplete([this](uint32_t complets) {
      return this->writeComplete(complets);
    });
  }

  this->mRdmaConnection->setOnIncomingPacketPackReady([this](RDMAIncomingPacket *packet) {
    return this->packReady(packet);
  });

  this->mWriteBatchsize = __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__;
  mOnIncomingPacketBuild = NULL;
  mIncomingPacket = new RDMAIncomingPacket(1024*1024*10);
  mCausedBackPressure = false;
  pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
}

HeronRDMAConnection::~HeronRDMAConnection() { }

int32_t HeronRDMAConnection::sendPacket(RDMAOutgoingPacket* packet) {
  return sendPacket(packet, NULL);
}

void HeronRDMAConnection::packReady(RDMAIncomingPacket *packet) {
  if (mOnIncomingPacketBuild) {
    mOnIncomingPacketBuild(packet);
  }
}

int32_t HeronRDMAConnection::sendPacket(RDMAOutgoingPacket* packet,
                                        VCallback<NetworkErrorCode> cb) {
  packet->PrepareForWriting();
  //if (registerForWrite() != 0) return -1;
  // LOG(INFO) << "Connect LOCK";
  pthread_spin_lock(&lock);
  mOutstandingPackets.push_back(std::make_pair(packet, std::move(cb)));
  mNumOutstandingPackets++;
//  mNumOutstandingBytes += packet->GetTotalPacketSize();

//  if (!hasCausedBackPressure()) {
//    // Are we above the threshold?
//    if (mNumOutstandingBytes >= systemHWMOutstandingBytes) {
//      // Have we been above the threshold enough number of times?
//      if (++mNumEnqueuesWithBufferFull > __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__) {
//        mNumEnqueuesWithBufferFull = 0;
//        if (mOnConnectionBufferFull) {
//          mOnConnectionBufferFull(this);
//        }
//      }
//    } else {
//      mNumEnqueuesWithBufferFull = 0;
//    }
//  }
  // LOG(INFO) << "Connect Un-LOCK";
  pthread_spin_unlock(&lock);
//  if (channel_type == READ_WRITE) {
//    writeIntoEndPoint(0);
//  }
  return 0;
}

void HeronRDMAConnection::registerForNewPacket(VCallback<RDMAIncomingPacket*> cb) {
  mOnNewPacket = std::move(cb);
}

void HeronRDMAConnection::registerForPacking(VCallback<RDMAIncomingPacket*> cb) {
  mOnIncomingPacketBuild = std::move(cb);
}

int32_t HeronRDMAConnection::registerForBackPressure(VCallback<HeronRDMAConnection*> cbStarter,
                                                     VCallback<HeronRDMAConnection*> cbReliever) {
  mOnConnectionBufferFull = std::move(cbStarter);
  mOnConnectionBufferEmpty = std::move(cbReliever);
  return 0;
}

int HeronRDMAConnection::writeComplete(ssize_t numWritten) {
//  mNumOutstandingBytes -= numWritten;
//  LOG(INFO) << "Write complete " << numWritten;
  //pthread_spin_lock(&lock);
  while (numWritten > 0 && mNumPendingWritePackets > 0) {
    auto pr = mPendingPackets.front();
    int32_t bytesLeftForThisPacket = pr.first->GetTotalPacketSize() - pr.first->position_;
    // This iov structure was completely written as instructed
    if (numWritten >= bytesLeftForThisPacket) {
//      LOG(INFO) << "Write confirmed for packet";
      // This whole packet has been consumed
      // mSentPackets.push_back(pr);
      mPendingPackets.pop_front();
      mNumOutstandingPackets--;
      mNumPendingWritePackets--;
      numWritten -= bytesLeftForThisPacket;
      if (pr.second) {
        pr.second(OK);
      } else {
//        if (pr.first->direct_proto_) {
//          delete pr.first->_proto;
//        }
        delete pr.first;
      }
    } else {
      // This iov structure has been partially sent out
      pr.first->position_ += numWritten;
      numWritten = 0;
    }
  }

//  // Check if we reduced the write buffer to something below the back
//  // pressure threshold
//  if (hasCausedBackPressure()) {
//    // Signal pipe free
//    if (mNumOutstandingBytes <= systemLWMOutstandingBytes) {
//      mOnConnectionBufferEmpty(this);
//    }
//  }
  // LOG(INFO) << "Connect Un-LOCK";
  //pthread_spin_unlock(&lock);
  return 0;
}

bool HeronRDMAConnection::stillHaveDataToWrite() {
  return !mOutstandingPackets.empty();
}

int32_t HeronRDMAConnection::writeIntoEndPoint(int fd) {
  uint32_t size_to_write = 0;
  char *buf = NULL;
  uint32_t current_write = 0, total_write = 0;
  int write_status = 0;
  // LOG(INFO) << "Write to endpoint";
//  int current_packet = 0;
  // LOG(INFO) << "Connect LOCK";
  pthread_spin_lock(&lock);
  int packets = 0;
//  for (auto iter = mOutstandingPackets.begin(); iter != mOutstandingPackets.end(); ++iter) {
  while (mOutstandingPackets.size() > 0) {
    // LOG(INFO) << "Write data";
    auto iter = mOutstandingPackets.front();
    packets++;

    size_to_write = iter.first->GetTotalPacketSize() - iter.first->position_;
    // try to write the data
    if (iter.first->position_ == 0) {
      uint32_t max_writable_buf = maxWritableBufferSize();
      if (iter.first->direct_proto_) {
        if (max_writable_buf >= size_to_write) {
          write_status = writeData(iter.first, &current_write);
        } else {
          if (!iter.first->packed_) {
            iter.first->Pack();
          }
          buf = iter.first->get_header() + iter.first->position_;
          write_status = writeData((uint8_t *) buf, size_to_write, &current_write);
        }
      } else {
        buf = iter.first->get_header() + iter.first->position_;
        write_status = writeData((uint8_t *) buf, size_to_write, &current_write);
      }
    } else {
      buf = iter.first->get_header() + iter.first->position_;
      write_status = writeData((uint8_t *) buf, size_to_write, &current_write);
    }
//    LOG(INFO) << "current_packet=" << current_packet << " size_to_write="
//      << size_to_write << " current_write=" << current_write << "remain: "
//      << mOutstandingPackets.size();
    if (write_status) {
      LOG(ERROR) << "Failed to write the data";
      pthread_spin_unlock(&lock);
      return write_status;
    }

    // we have written this fully to the buffers
    if (current_write == size_to_write) {
      mNumPendingWritePackets++;
      mPendingPackets.push_back(iter);
      mOutstandingPackets.pop_front();
      iter.first->position_ = 0;
//      LOG(INFO) << "Finished writing packet";
    } else {
      // partial write
      iter.first->position_ += current_write;
    }

    // iter++;
    total_write += current_write;
    // we loop until we write everything we want to write is successful
    // and total written data is less than batch size
    if (!(current_write == size_to_write && total_write < mWriteBatchsize)) {
      // LOG(INFO) << "Break write" << current_write;
      break;
    }
  }
//  LOG(ERROR) << "Iterated through: " << packets;
  pthread_spin_unlock(&lock);
  return 0;
}

int32_t HeronRDMAConnection::readFromEndPoint(int fd) {
  int32_t bytesRead = 0;
  while (1) {
    int32_t read_status = ReadPacket();
    if (read_status == 0) {
      // Packet was succcessfully read.
      RDMAIncomingPacket *packet = mIncomingPacket;
      mIncomingPacket = new RDMAIncomingPacket(mRdmaOptions->max_packet_size_);
      mReceivedPackets.push_back(packet);
      bytesRead += packet->GetTotalPacketSize();
      if (bytesRead >= __SYSTEM_NETWORK_READ_BATCH_SIZE__) {
        return 0;
      }
    } else if (read_status > 0) {
      // packet was read partially
      return 0;
    } else {
      return -1;
    }
  }
  return 0;
}

int32_t HeronRDMAConnection::ReadPacket() {
  uint32_t read = 0;
  if (!mIncomingPacket->headerReadDone) {
    // We are still reading the header
    int32_t read_status = 0;
    read_status = readData((uint8_t *) (mIncomingPacket->header_ + mIncomingPacket->position_),
                           RDMAPacketHeader::header_size() - mIncomingPacket->position_, &read);
    if (read_status != 0) {
      // Header read is either partial or had an error
      return read_status;
    } else {
      // if we read something
      if (read > 0) {
        mIncomingPacket->position_ += read;
        // now check weather we have read every thing
        if (mIncomingPacket->position_ == RDMAPacketHeader::header_size()) {
          // Header just completed - some sanity checking of the header
          if (mIncomingPacket->max_packet_size_ != 0 &&
              RDMAPacketHeader::get_packet_size(mIncomingPacket->header_)
              > mIncomingPacket->max_packet_size_) {
            // Too large packet
            LOG(ERROR) << "Too large packet size "
                       << RDMAPacketHeader::get_packet_size(mIncomingPacket->header_)
                       << ". We only accept packet sizes <= "
                       << mIncomingPacket->max_packet_size_ << "\n";
            return -1;
          } else {
            // Create the data
            uint32_t max_writable_buf = maxWritableBufferSize();
            uint32_t length = RDMAPacketHeader::get_packet_size(mIncomingPacket->header_);
            mIncomingPacket->_proto = NULL;
            if (max_writable_buf <  length) {
              mIncomingPacket->data_ =
                            new char[length];
              mIncomingPacket->direct_proto_ = false;
            } else {
              mIncomingPacket->direct_proto_ = true;
            }
            // reset the position to refer to the data_
            mIncomingPacket->position_ = 0;
            mIncomingPacket->headerReadDone = true;
            // we need to read this much data
            // return RDMAPacketHeader::get_packet_size(mIncomingPacket->header_);
          }
        }
      }
      // return RDMAPacketHeader::header_size() - mIncomingPacket->position_;
    }
  }

  if (mIncomingPacket->headerReadDone) {
    // The header has been completely read. Read the data
    int32_t retval = 0;
    if (mIncomingPacket->direct_proto_) {
      return readData(mIncomingPacket, &read);
    } else {
      retval = readData((uint8_t *) (mIncomingPacket->data_ + mIncomingPacket->position_),
                        RDMAPacketHeader::get_packet_size(mIncomingPacket->header_) -
                        mIncomingPacket->position_, &read);
      if (retval != 0) {
        return retval;
      } else {
        // now check weather we have read evrything we need
        mIncomingPacket->position_ += read;
        if (RDMAPacketHeader::get_packet_size(mIncomingPacket->header_) ==
            mIncomingPacket->position_) {
          mIncomingPacket->position_ = 0;
          return 0;
        } else {
          return RDMAPacketHeader::get_packet_size(mIncomingPacket->header_)
                 - mIncomingPacket->position_;
        }
      }
    }

  }
  return 1;
}

void HeronRDMAConnection::handleDataRead() {
  while (!mReceivedPackets.empty()) {
    RDMAIncomingPacket* packet = mReceivedPackets.front();
    if (mOnNewPacket) {
      mOnNewPacket(packet);
    } else {
      LOG(WARNING) << "Packet read complete and dropping";
      delete packet;
    }
    mReceivedPackets.pop_front();
  }
}
