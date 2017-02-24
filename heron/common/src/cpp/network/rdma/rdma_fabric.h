#ifndef RDMA_FABRIC_H_
#define RDMA_FABRIC_H_

#include "network/rdma/utils.h"
#include "network/rdma/options.h"

#define VERBS_PROVIDER "verbs"
#define PSM2_PROVIDER "psm2"

class RDMAFabric {
public:
  RDMAFabric(RDMAOptions *options);
  int Init();

  struct fi_info *GetHints() {
    return info_hints;
  }

  struct fi_info *GetInfo() {
    return info;
  }

  struct fid_fabric *GetFabric() {
    return fabric;
  }
private:
  RDMAOptions *options;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the fabric
  struct fid_fabric *fabric;
  // fabric information obtained
  struct fi_info *info;
};

#endif