#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cstring>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <poll.h>

#include "utils.h"

char default_port[8] = "9228";

int hps_utils_get_cq_fd(RDMAOptions *opts, struct fid_cq *cq, int *fd) {
  int ret = FI_SUCCESS;
  if (cq) {
    ret = fi_control(&cq->fid, FI_GETWAIT, fd);
    if (ret) {
      HPS_ERR("fi_control(FI_GETWAIT) %d", ret);
    }
  }
  return ret;
}

int hps_utils_get_eq_fd(RDMAOptions *opts, struct fid_eq *eq, int *fd) {
  int ret = FI_SUCCESS;
  if (eq) {
    ret = fi_control(&eq->fid, FI_GETWAIT, fd);
    if (ret) {
      HPS_ERR("fi_control(FI_GETWAIT) %d", ret);
    }
  }
  return ret;
}

static int hps_utils_dupaddr(void **dst_addr, size_t *dst_addrlen,
                              void *src_addr, size_t src_addrlen) {
  *dst_addr = malloc(src_addrlen);
  *dst_addrlen = src_addrlen;
  memcpy(*dst_addr, src_addr, src_addrlen);
  return 0;
}

/**
 * Get the address according to the format supported by the device
 */
static int hps_utils_getaddr(char *node, char *service,
                             struct fi_info *hints, uint64_t flags) {
  int ret;
  struct fi_info *fi;

  if (!node && !service) {
    if (flags & FI_SOURCE) {
      hints->src_addr = NULL;
      hints->src_addrlen = 0;
    } else {
      hints->dest_addr = NULL;
      hints->dest_addrlen = 0;
    }
    return 0;
  }

  printf("Get info with options node=%s service=%s flags=%d\n", node, service, (int)flags);

  ret = fi_getinfo(HPS_FIVERSION, node, service, flags, hints, &fi);
  if (ret) {
    return ret;
  }
  hints->addr_format = fi->addr_format;

  if (flags & FI_SOURCE) {
    ret = hps_utils_dupaddr(&hints->src_addr, &hints->src_addrlen,
                             fi->src_addr, fi->src_addrlen);
  } else {
    ret = hps_utils_dupaddr(&hints->dest_addr, &hints->dest_addrlen,
                             fi->dest_addr, fi->dest_addrlen);
  }

  fi_freeinfo(fi);
  return ret;
}

int hps_utils_read_addr_opts(char **node, char **service, struct fi_info *hints,
                              uint64_t *flags, RDMAOptions *opts) {
  int ret;

  if (opts->dst_addr) {
    if (!opts->dst_port) {
      opts->dst_port = default_port;
    }

    ret = hps_utils_getaddr(opts->src_addr, opts->src_port, hints, FI_SOURCE);
    if (ret) {
      return ret;
    }
    *node = opts->dst_addr;
    *service = opts->dst_port;
  } else {
    if (!opts->src_port) {
      opts->src_port = default_port;
    }
    *node = opts->src_addr;
    *service = opts->src_port;
    *flags = FI_SOURCE;
  }

  return 0;
}

void print_info(struct fi_info *info) {
  char *out = fi_tostr(info, FI_TYPE_INFO);
  printf(out);
}

int print_short_info(struct fi_info *info) {
  for (struct fi_info *cur = info; cur; cur = cur->next) {
    printf("provider: %s\n", cur->fabric_attr->prov_name);
    printf("    fabric: %s\n", cur->fabric_attr->name),
        printf("    domain: %s\n", cur->domain_attr->name),
        printf("    version: %d.%d\n", FI_MAJOR(cur->fabric_attr->prov_version),
               FI_MINOR(cur->fabric_attr->prov_version));
  }
  return EXIT_SUCCESS;
}


int hps_utils_get_info(RDMAOptions *options, struct fi_info *hints, struct fi_info **info) {
  // char *fi_str;
  char *node, *service;
  uint64_t flags = 0;

  // read the parameters from the options
  hps_utils_read_addr_opts(&node, &service, hints, &flags, options);

  // default to RDM
  if (!hints->ep_attr->type) {
    hints->ep_attr->type = FI_EP_RDM;
  }

  // now lets retrieve the available network services
  // according to hints
  int ret = fi_getinfo(HPS_FIVERSION, node, service, flags, hints, info);
  if (ret) {
    HPS_ERR("Fi_info failed %d", ret);
    return 1;
  }

  return 0;
}

uint64_t hps_utils_caps_to_mr_access(uint64_t caps) {
  uint64_t mr_access = 0;

  if (caps & (FI_MSG | FI_TAGGED)) {
    if (caps & HPS_MSG_MR_ACCESS)
      mr_access |= caps & HPS_MSG_MR_ACCESS;
    else
      mr_access |= HPS_MSG_MR_ACCESS;
  }

  if (caps & (FI_RMA | FI_ATOMIC)) {
    if (caps & HPS_RMA_MR_ACCESS)
      mr_access |= caps & HPS_RMA_MR_ACCESS;
    else
      mr_access |= HPS_RMA_MR_ACCESS;
  }

  return mr_access;
}

int hps_utils_poll_fd(int fd, int timeout) {
  struct pollfd fds;
  int ret;

  fds.fd = fd;
  fds.events = POLLIN;
  ret = poll(&fds, 1, timeout);
  if (ret == -1) {
    HPS_ERR("poll %d", -errno);
    ret = -errno;
  } else if (!ret) {
    ret = -FI_EAGAIN;
  } else {
    ret = 0;
  }
  return ret;
}

int hps_utils_cq_readerr(struct fid_cq *cq){
  struct fi_cq_err_entry cq_err;
  ssize_t ret;

  ret = fi_cq_readerr(cq, &cq_err, 0);
  if (ret < 0) {
    HPS_ERR("fi_cq_readerr %ld", ret);
  } else {
    printf("%s\n", fi_cq_strerror(cq, cq_err.prov_errno,
                                  cq_err.err_data, NULL, 0));
    ret = -cq_err.err;
  }
  return (int) ret;
}

