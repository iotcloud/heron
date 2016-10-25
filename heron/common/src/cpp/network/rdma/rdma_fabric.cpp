
#include <glog/logging.h>
#include "rdma_fabric.h"

#define VERBS_PROVIDER "verbs"

RDMAFabric::RDMAFabric(RDMAOptions *options) {
  this->options = options;
}

int RDMAFabric::Init() {
  int ret;
  info_hints = fi_allocinfo();
  // we are going to use verbs provider
  info_hints->fabric_attr->prov_name = strdup(VERBS_PROVIDER);
  info_hints->ep_attr->type = FI_EP_MSG;
  info_hints->caps = FI_MSG | FI_RMA;
  info_hints->mode = FI_LOCAL_MR | FI_RX_CQ_DATA;
  print_info(info_hints);

  ret = hps_utils_get_info(this->options, this->info_hints, &this->info);
  if (ret) {
    return ret;
  }

  printf("Fabric info:\n");
  print_info(this->info);

  ret = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
  if (ret) {
    LOG(ERROR) << "Failed to create fabric " << ret;
    return ret;
  }

  return ret;
}
