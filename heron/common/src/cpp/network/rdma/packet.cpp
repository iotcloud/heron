#include <netinet/in.h>
#include <glog/logging.h>
#include "packet.h"
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>

// PacketHeader static methods
void RDMAPacketHeader::set_packet_size(char* header, uint32_t _size) {
  uint32_t network_order = htonl(_size);
  memcpy(header, &network_order, sizeof(uint32_t));
}

uint32_t RDMAPacketHeader::get_packet_size(const char* header) {
  uint32_t network_order = *(reinterpret_cast<const uint32_t*>(header));
  return ntohl(network_order);
}

uint32_t RDMAPacketHeader::header_size() { return kSPPacketSize; }

// Constructor of the IncomingPacket. We only create the header buffer.
RDMAIncomingPacket::RDMAIncomingPacket(uint32_t _max_packet_size) {
  max_packet_size_ = _max_packet_size;
  position_ = 0;
  // bzero(header_, PacketHeader::size());
  data_ = NULL;
}

// Construct an incoming from a raw data buffer - used for tests only
RDMAIncomingPacket::RDMAIncomingPacket(char* _data) {
  memcpy(header_, _data, RDMAPacketHeader::header_size());
  data_ = new char[RDMAPacketHeader::get_packet_size(header_)];
  memcpy(data_, _data + RDMAPacketHeader::header_size(), RDMAPacketHeader::get_packet_size(header_));
  position_ = 0;
}

RDMAIncomingPacket::~RDMAIncomingPacket() { delete[] data_; }

int32_t RDMAIncomingPacket::UnPackInt(int32_t* i) {
  if (data_ == NULL) return -1;
  if (position_ + sizeof(int32_t) > RDMAPacketHeader::get_packet_size(header_)) {
    LOG(ERROR) << "position + 4: " << (position_ + sizeof(int32_t)) << " packet size:" << RDMAPacketHeader::get_packet_size(header_);
    return -1;
  }
  uint32_t network_order;
  memcpy(&network_order, data_ + position_, sizeof(int32_t));
  position_ += sizeof(int32_t);
  *i = ntohl(network_order);
  return 0;
}

int32_t RDMAIncomingPacket::UnPackString(std::string* i) {
  int32_t size = 0;
  if (UnPackInt(&size) != 0) return -1;
  if (position_ + size > RDMAPacketHeader::get_packet_size(header_)) {
     LOG(ERROR) << "position + 4: " << (position_ + sizeof(int32_t)) << " packet size:" << RDMAPacketHeader::get_packet_size(header_);
    return -1;
  }
  *i = std::string(data_ + position_, size);
  position_ += size;
  return 0;
}

int32_t RDMAIncomingPacket::UnPackProtocolBuffer(google::protobuf::Message* _proto) {
  int32_t sz;
  if (UnPackInt(&sz) != 0) return -1;
  if (position_ + sz > RDMAPacketHeader::get_packet_size(header_)) return -1;
  if (!_proto->ParseFromArray(data_ + position_, sz)) return -1;
  position_ += sz;
  return 0;
}

int32_t RDMAIncomingPacket::UnPackREQID(REQID* _rid) {
  if (position_ + REQID_size > RDMAPacketHeader::get_packet_size(header_)) return -1;
  _rid->assign(std::string(data_ + position_, REQID_size));
  position_ += REQID_size;
  return 0;
}

uint32_t RDMAIncomingPacket::GetTotalPacketSize() const {
  return RDMAPacketHeader::get_packet_size(header_) + kSPPacketSize;
}

void RDMAIncomingPacket::Reset() { position_ = 0; }

RDMAOutgoingPacket::RDMAOutgoingPacket(uint32_t _packet_size) {
  total_packet_size_ = _packet_size + RDMAPacketHeader::header_size();
  data_ = new char[total_packet_size_];
  RDMAPacketHeader::set_packet_size(data_, _packet_size);
  position_ = RDMAPacketHeader::header_size();
}

RDMAOutgoingPacket::~RDMAOutgoingPacket() { delete[] data_; }

uint32_t RDMAOutgoingPacket::GetTotalPacketSize() const { return total_packet_size_; }

uint32_t RDMAOutgoingPacket::GetBytesFilled() const { return position_; }

uint32_t RDMAOutgoingPacket::GetBytesLeft() const { return total_packet_size_ - position_; }

int32_t RDMAOutgoingPacket::PackInt(const int32_t& i) {
  if (sizeof(int32_t) + position_ > total_packet_size_) {
    return -1;
  }
  int32_t network_order = htonl(i);
  memcpy(data_ + position_, &network_order, sizeof(int32_t));
  position_ += sizeof(int32_t);
  return 0;
}

uint32_t RDMAOutgoingPacket::SizeRequiredToPackProtocolBuffer(int32_t _byte_size) {
  return sizeof(int32_t) + _byte_size;
}

int32_t RDMAOutgoingPacket::PackProtocolBuffer(const google::protobuf::Message& _proto,
                                            int32_t _byte_size) {
  if (PackInt(_byte_size) != 0) return -1;
  if (_byte_size + position_ > total_packet_size_) {
    return -1;
  }
  if (!_proto.SerializeToArray(data_ + position_, _byte_size)) return -1;
  position_ += _byte_size;
  return 0;
}

int32_t RDMAOutgoingPacket::PackREQID(const REQID& _rid) {
  if (REQID_size + position_ > total_packet_size_) {
    return -1;
  }
  memcpy(data_ + position_, _rid.c_str(), REQID_size);
  position_ += REQID_size;
  return 0;
}

uint32_t RDMAOutgoingPacket::SizeRequiredToPackString(const std::string& _input) {
  return sizeof(uint32_t) + _input.size();
}

int32_t RDMAOutgoingPacket::PackString(const string& i) {
  if (sizeof(uint32_t) + i.size() + position_ > total_packet_size_) {
    return -1;
  }
  PackInt(i.size());
  memcpy(data_ + position_, i.c_str(), i.size());
  position_ += i.size();
  return 0;
}

void RDMAOutgoingPacket::PrepareForWriting() {
  CHECK(position_ == total_packet_size_);
  position_ = 0;
}
