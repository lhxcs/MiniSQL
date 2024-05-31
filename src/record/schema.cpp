#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t cnt = 0;
  MACH_WRITE_UINT32(buf + cnt, SCHEMA_MAGIC_NUM);
  cnt += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + cnt, GetColumnCount());
  cnt += sizeof(uint32_t);
  for (uint32_t i = 0; i < GetColumnCount(); i++) {
    cnt += columns_[i]->SerializeTo(buf + cnt);
  }
  return cnt;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t cnt = 2 * sizeof(uint32_t);
  for (uint32_t i = 0; i < GetColumnCount(); i++) {
    cnt += columns_[i]->GetSerializedSize();
  }
  return cnt;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t cnt = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf + cnt);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "schema is wrong!");
  cnt += sizeof(uint32_t);
  uint32_t column_num = MACH_READ_UINT32(buf + cnt);
  cnt += sizeof(uint32_t);
  std::vector<Column *> cols;
  cols.resize(column_num);
  for (uint32_t i = 0; i < column_num; i++) {
    Column *col;
    cnt += Column::DeserializeFrom(buf + cnt, col);
    cols.push_back(col);
  }
  schema = new Schema(cols);
  return cnt;
}