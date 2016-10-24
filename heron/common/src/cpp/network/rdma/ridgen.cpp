#include "ridgen.h"
#include <sstream>
#include <string>
#include <uuid/uuid.h>

void REQID::assign(const std::string& _id) {
  id_ = _id;
}

REQID_Generator::REQID_Generator() {
}

REQID_Generator::~REQID_Generator() {
}

REQID
REQID_Generator::generate() {
  uuid_t uuid;
  uuid_generate_random(uuid);
  char s[37];
  uuid_unparse(uuid, s);
  // Convert into string
  std::ostringstream ss;
  ss << new std::string(s);
  return REQID(ss.str());
}

REQID
REQID_Generator::generate_zero_reqid() { return REQID(); }