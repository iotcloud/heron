#ifndef HPS_H_
#define HPS_H_

#include <cstdio>
#include <cstdlib>

#define HPS_LOG(level, fmt, ...) \
	do { fprintf(stdout, "[%s] hps:%s:%d: " fmt "\n", level, __FILE__, \
			__LINE__, ##__VA_ARGS__); } while (0)

#define HPS_LOG_INFO(level, fmt, ...) \
	do { fprintf(stdout, "[%s] hps:%s:%d: " fmt "\n", level, __FILE__, \
			__LINE__, ##__VA_ARGS__); } while (0)

#define HPS_ERR(fmt, ...) HPS_LOG("error", fmt, ##__VA_ARGS__)
//#define HPS_ERR(fmt, ...)
#define HPS_WARN(fmt, ...) HPS_LOG("warn", fmt, ##__VA_ARGS__)
//#define HPS_WARN(fmt, ...)
#define HPS_INFO(fmt, ...) HPS_LOG("info", fmt, ##__VA_ARGS__)
//#define HPS_INFO(fmt, ...)

#define MAX_ERRORS 10

enum hps_comp_method {
  HPS_COMP_SPIN = 0,
  HPS_COMP_SREAD,
  HPS_COMP_WAITSET,
  HPS_COMP_WAIT_FD
};

enum hps_rma_opcodes {
  HPS_RMA_READ = 1,
  HPS_RMA_WRITE,
  HPS_RMA_WRITEDATA,
};

enum {
  HPS_OPT_ACTIVE		= 1 << 0,
  HPS_OPT_ITER		= 1 << 1,
  HPS_OPT_SIZE		= 1 << 2,
  HPS_OPT_RX_CQ		= 1 << 3,
  HPS_OPT_TX_CQ		= 1 << 4,
  HPS_OPT_RX_CNTR		= 1 << 5,
  HPS_OPT_TX_CNTR		= 1 << 6,
  HPS_OPT_VERIFY_DATA	= 1 << 7,
  HPS_OPT_ALIGN		= 1 << 8,
  HPS_OPT_BW		= 1 << 9,
};

#ifndef HPS_FIVERSION
#define HPS_FIVERSION FI_VERSION(1,3)
#endif

#define MAX(a,b) (((a)>(b))?(a):(b))

#define HPS_MAX_CTRL_MSG 64
#define HPS_STR_LEN 32
#define HPS_MR_KEY 0xD0DE
#define HPS_MR_KEY_W 0xC0DF

#define HPS_MSG_MR_ACCESS (FI_SEND | FI_RECV)
#define HPS_RMA_MR_ACCESS (FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE)

#endif /* HPS_H */