
#include <glog/logging.h>
#include "network/rdma/rdma_fabric.h"

RDMAFabric::RDMAFabric(RDMAOptions *options) {
  this->options = options;
}

int RDMAFabric::Init() {
  int ret;
  info_hints = fi_allocinfo();
  // we are going to use verbs provider
  if (options->provider == VERBS_PROVIDER_TYPE) {
    info_hints->fabric_attr->prov_name = strdup(VERBS_PROVIDER);
    info_hints->ep_attr->type = FI_EP_MSG;
    info_hints->caps = FI_MSG | FI_RMA;
    info_hints->mode = FI_LOCAL_MR | FI_RX_CQ_DATA;
    info_hints->ep_attr->rx_ctx_cnt = 64;
    info_hints->ep_attr->tx_ctx_cnt = 64;
    info_hints->tx_attr->size = 64;
    info_hints->rx_attr->size = 64;
  } else if (options->provider == PSM2_PROVIDER_TYPE) {
    info_hints->fabric_attr->prov_name = strdup(PSM2_PROVIDER);
    info_hints->ep_attr->type = FI_EP_RDM;
    info_hints->caps = FI_TAGGED | FI_RMA;
    info_hints->mode = FI_CONTEXT | FI_LOCAL_MR | FI_RX_CQ_DATA;
  }
  ret = hps_utils_get_info(this->options, this->info_hints, &this->info);
  if (ret) {
    return ret;
  }

  ret = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
  if (ret) {
    LOG(ERROR) << "Failed to create fabric " << ret;
    return ret;
  }

  return ret;
}
