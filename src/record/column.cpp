#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t cnt;
  MACH_WRITE_UINT32(buf + cnt, COLUMN_MAGIC_NUM);
  cnt += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + cnt, name_.length());
  cnt += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + cnt, name_);
  cnt += name_.length();
  MACH_WRITE_TO(TypeId, buf + cnt, type_);
  cnt += sizeof(TypeId);
  MACH_WRITE_UINT32(buf + cnt, len_);
  cnt += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + cnt, table_ind_);
  cnt += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + cnt, nullable_);
  cnt += sizeof(bool);
  MACH_WRITE_TO(bool, buf + cnt, unique_);
  cnt += sizeof(bool);
  return cnt;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t cnt = 0;
  cnt += name_.length() + 4 * sizeof(uint32_t) + sizeof(TypeId) + 2 * sizeof(bool);
  return cnt;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  int cnt = 0;
  uint32_t MAGIC_NUM = MACH_READ_INT32(buf + cnt);
  cnt += sizeof(uint32_t);
  ASSERT(MAGIC_NUM == Column::COLUMN_MAGIC_NUM, "column is wrong!");
  uint32_t name_length = MACH_READ_UINT32(buf + cnt);
  cnt += sizeof(uint32_t);
  char name[name_length];
  memcpy(name, buf + cnt, name_length);
  std::string fname(name);
  cnt += name_length;
  TypeId type = MACH_READ_FROM(TypeId, buf + cnt);
  cnt += sizeof(TypeId);
  uint32_t len = MACH_READ_UINT32(buf + cnt);
  cnt += sizeof(uint32_t);
  uint32_t table_ind = MACH_READ_UINT32(buf + cnt);
  cnt += sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool, buf + cnt);
  cnt += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf + cnt);
  cnt += sizeof(bool);
  if (type == TypeId::kTypeChar)
    column = new Column(std::string(name), type, len, table_ind, nullable, unique);
  else
    column = new Column(std::string(name), type, table_ind, nullable, unique);
  return cnt;
}
