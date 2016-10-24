#if !defined(__RID_GEN_H)
#define __RID_GEN_H

#include <string>
#include <vector>
#include "sptypes.h"

const uint32_t REQID_size = 32;

class REQID {
 public:
  //! Constructors
  REQID() : id_(REQID_size, 0) {}

  //! Destructors
  ~REQID(){};

  //! Overload the assignment operators
  REQID& operator=(const REQID& _reqid) {
    id_ = _reqid.id_;
    return *this;
  }
  void assign(const std::string& _id);

  //! Get the underlying string representation
  const std::string& str() const { return id_; }

  //! Get the underlying C string representation
  const char* c_str() const { return id_.data(); }

  //! Clear the reqid
  void clear() { id_.clear(); }

  //! Get the length of the request id
  static uint32_t length() { return REQID_size; }

 private:
  //! Private constructor used by generator
  explicit REQID(const std::string& _id) : id_(_id.c_str(), length()) {}

  //! Underlying representation of request ID
  std::string id_;

  friend bool operator==(const REQID& lhs, const REQID& rhs);
  friend bool operator!=(const REQID& lhs, const REQID& rhs);
  friend std::ostream& operator<<(std::ostream& _os, const REQID& _reqid);

  friend class REQID_Generator;
};

typedef std::vector<REQID> REQID_Vector;
typedef REQID_Vector::iterator REQID_Vector_Iterator;

inline bool operator==(const REQID& lhs, const REQID& rhs) {
  return lhs.id_ == rhs.id_;
}

inline bool operator!=(const REQID& lhs, const REQID& rhs) {
  return lhs.id_ != rhs.id_;
}

inline std::ostream& operator<<(std::ostream& _os, const REQID& _reqid) {
  _os << _reqid.id_;
  return _os;
}

class REQID_Hash {
public:
  size_t operator()(const REQID& x) const { return std::hash<std::string>()(x.str()); }
};

class REQID_Generator {
 public:
  //! Constructor that uses underlying UUID library
  REQID_Generator();

  //! Destructor
  ~REQID_Generator();

  //! Generate a request ID
  REQID generate();

  //! Return a zero REQID
  static REQID generate_zero_reqid();

 private:
  void* rands_;  //! Random stream
};

#endif
