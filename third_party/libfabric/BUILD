licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "libfabric"

package_version = "1.3.0"

package_file = package_name + "-" + package_version + ".tar.gz"

package_dir = package_name + "-" + package_version

genrule(
    name = "libfabric-srcs",
    srcs = [
        package_file,
    ],
    outs = [
        "include/rdma/fabric.h",
        "include/rdma/fi_atomic.h",
        "include/rdma/fi_cm.h",
        "include/rdma/fi_domain.h",
        "include/rdma/fi_endpoint.h",
        "include/rdma/fi_eq.h",
        "include/rdma/fi_errno.h",
        "include/rdma/fi_rma.h",
        "include/rdma/fi_tagged.h",
        "include/rdma/fi_trigger.h",
        #"include/rdma/providers/fi_log.h",
        #"include/rdma/providers/fi_prov.h",
        "lib/libfabric.a",
    ],
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t libfabric.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir,
        #"sh autogen.sh",
        "./configure --prefix=$$INSTALL_DIR --enable-shared=no LDFLAGS=\"-Wl,--no-as-needed -ldl\"",
        "make",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "libfabric-cxx",
    srcs = [
        "include/rdma/fabric.h",
        "include/rdma/fi_atomic.h",
        "include/rdma/fi_cm.h",
        "include/rdma/fi_domain.h",
        "include/rdma/fi_endpoint.h",
        "include/rdma/fi_eq.h",
        "include/rdma/fi_errno.h",
        "include/rdma/fi_rma.h",
        "include/rdma/fi_tagged.h",
        "include/rdma/fi_trigger.h",
        #"include/rdma/providers/fi_log.h",
        #"include/rdma/providers/fi_prov.h",
        "empty.cc",
        "lib/libfabric.a",
    ],
    hdrs = [
        "include/rdma/fabric.h",
        "include/rdma/fi_atomic.h",
        "include/rdma/fi_cm.h",
        "include/rdma/fi_domain.h",
        "include/rdma/fi_endpoint.h",
        "include/rdma/fi_eq.h",
        "include/rdma/fi_errno.h",
        "include/rdma/fi_rma.h",
        "include/rdma/fi_tagged.h",
        "include/rdma/fi_trigger.h",
        #"include/rdma/providers/fi_log.h",
        #"include/rdma/providers/fi_prov.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)
