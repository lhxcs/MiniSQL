#include "record/schema.h"
#include <iostream>
using namespace std;

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t cnt = 0;
  MACH_WRITE_UINT32(buf + cnt, SCHEMA_MAGIC_NUM);
  cnt += sizeof(uint32_t);
  MACH_WRITE_TO(size_t, buf + cnt, columns_.size());
  cnt += sizeof(size_t);
  for (uint32_t i = 0; i < columns_.size(); i++) {
    cnt += columns_[i]->SerializeTo(buf + cnt);
  }
  MACH_WRITE_TO(bool, buf + cnt, is_manage_);
  cnt += sizeof(bool);
  return cnt;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t cnt = sizeof(bool) + sizeof(size_t);
  cnt += sizeof(uint32_t);
  for (uint32_t i = 0; i < columns_.size(); i++) {
    cnt += columns_[i]->GetSerializedSize();
  }
  //cout<< "Schema::GetSerializedSize: " << cnt << endl;
  return cnt;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if(schema != nullptr) {
    return 0;
  }
  delete schema;
  uint32_t cnt = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf + cnt);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "schema is wrong!");
  cnt += sizeof(uint32_t);
  uint32_t column_num = MACH_READ_FROM(size_t, buf + cnt);
  cnt += sizeof(size_t);
  std::vector<Column *> cols;
  for (uint32_t i = 0; i < column_num; i++) {
    cols.push_back(nullptr);  
    cnt += Column::DeserializeFrom(buf + cnt, cols[i]);
  }
  bool is_manage = MACH_READ_FROM(bool, buf + cnt);
  cnt += sizeof(bool);
  schema = new Schema(cols, is_manage);
  if(schema == nullptr) {
    return 0;
  }
  return cnt;
}