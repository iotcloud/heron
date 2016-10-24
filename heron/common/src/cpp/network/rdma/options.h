#ifndef HPS_OPTIONS_H_
#define HPS_OPTIONS_H_

#include <cstdint>

#include "hps.h"

class RDMAOptions {
public:
  char *src_port;
  char *dst_port;
  char *src_addr;
  char *dst_addr;
  int options;
  uint32_t max_packet_size_;

  // buffer size of a individual buffer, if it is
  // smaller than minimum or greater that maximum supported,
  // it will be adjusted to the minimum
  size_t buf_size;
  // no of buffers
  uint32_t no_buffers;

  RDMAOptions();
  void Free();
private:
};

#endif /* HPS_OPTIONS_H_ */
