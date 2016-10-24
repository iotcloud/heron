#ifndef PACKET_H_
#define PACKET_H_

#include <functional>
#include <string>
#include <cstring>
#include <google/protobuf/message.h>
#include "ridgen.h"

using namespace google::protobuf;

namespace google {
  namespace protobuf {
    class Message;
  }
}

/*
 * The PacketHeader class defines a bunch of methods for the manipulation
 * of the Packet Header. Users pass the start of the header to its
 * functions. Always use PacketHeader class in dealing with a packet's
 * header instead of directly accessing the buffer.
 */

const uint32_t kSPPacketSize = sizeof(uint32_t);

class PacketHeader {
public:
  static void set_packet_size(char* header, uint32_t size);
  static uint32_t get_packet_size(const char* header);
  static uint32_t header_size();
};

/*
 * Class IncomingPacket - Definition of incoming packet
 *
 * Servers and clients use this structure to receive request/respones.
 * The underlying implementation uses two buffers. One for the header
 * and one for the actual data. We first read the header(since it is of
 * a fixed size. The header contain the data length information that allows
 * us to read the right amount of data. This implementation has the side
 * effect that we will need atleast two read calls to completely read a
 * packet.
 */
class IncomingPacket {
public:
  //! Constructor
  // We will read a packet of maximum max_packet_size len. A value of zero
  // implies no limit
  explicit IncomingPacket(uint32_t max_packet_size);

  // Form an incoming packet with raw data buffer - used for tests only
  explicit IncomingPacket(char* _data);

  //! Destructor
  ~IncomingPacket();

  // UnPacking functions
  // Unpacking functions return a zero on successful operation. A negative value
  // implies error in unpacking. This could indicate a garbled packet

  // unpack an integer
  int32_t UnPackInt(int32_t* i);

  // unpack a string
  int32_t UnPackString(std::string* i);

  // unpack a protocol buffer
  int32_t UnPackProtocolBuffer(google::protobuf::Message* _proto);

  // unpack a request id
  int32_t UnPackREQID(REQID* _rid);

  // resets the read pointer to the start of the data.
  void Reset();

  // gets the header
  char* get_header() { return header_; }

  // Get the total size of the packet
  uint32_t GetTotalPacketSize() const;

private:
  // Only Connection class can use the Read method to have
  // the packet read itself.
  friend class Connection;

  // Read the packet from the file descriptor fd.
  // Returns 0 if the packet has been read completely.
  // A > 0 return value indicates that the packet was
  // partially read and there was no more data. Further read
  // calls are necessary to completely read the packet.
  // A negative return value implies an irreovrable error
//  int32_t Read(Connection *fd);
//
//  // Helper method for Read to do the low level read calls.
//  int32_t InternalRead(Connection *con, char* buffer, uint32_t size);

  // The maximum packet length allowed. 0 means no limit
  uint32_t max_packet_size_;

  // The current read position.
  uint32_t position_;

  // The pointer to the header.
  char header_[kSPPacketSize];

  // The pointer to the data.
  char* data_;
};

/*
 * Class OutgoingPacket - Definition of outgoing packet
 *
 * The outgoing buffer is a contiguous stretch of data containing the header
 * and data. The current implementation mandates that user know the size of
 * the packet that they are sending.
 */
class OutgoingPacket {
public:
  // Constructor/Destructors.
  // Constructor takes in a packet size parameter. The packet data
  // size must be exactly equal to the size specified.
  explicit OutgoingPacket(uint32_t packet_size);
  ~OutgoingPacket();

  // Packing functions
  // A zero return value indicates a successful operation.
  // A negative value implies packing error.

  // pack an integer
  int32_t PackInt(const int32_t& i);

  // helper function to determine how much space is needed to encode a string
  static uint32_t SizeRequiredToPackString(const std::string& _input);

  // pack a string
  int32_t PackString(const std::string& i);

  // helper function to determine how much space is needed to encode a protobuf
  // The paramter byte_size is the whats reported by the ByteSize
  static uint32_t SizeRequiredToPackProtocolBuffer(int32_t _byte_size);

  // pack a proto buffer
  int32_t PackProtocolBuffer(const google::protobuf::Message& _proto, int32_t _byte_size);

  // pack a request id
  int32_t PackREQID(const REQID& _rid);

  // gets the header
  char* get_header() { return data_; }

  // gets the packet as a string
  const std::string toRawData();

  // Get the total size of the packet
  uint32_t GetTotalPacketSize() const;

  // get the current length of the packet
  uint32_t GetBytesFilled() const;

  // get the number of bytes left
  uint32_t GetBytesLeft() const;

private:
  // Only the Connection class can call the following functions
  friend class Connection;

  // Once the data has been packed, the packet needs to be prepared before sending.
  void PrepareForWriting();

  // The current position of packing/sending.
  uint32_t position_;

  // The header + data that makes the packet.
  char* data_;

  // The packet size as specified in the constructor.
  uint32_t total_packet_size_;
};

#endif  // PACKET_H_