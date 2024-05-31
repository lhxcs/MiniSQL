#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t cnt = 0;
  uint32_t fields_num = fields_.size();
  if(fields_num == 0) {
    return cnt;
  }
  MACH_WRITE_UINT32(buf + cnt, fields_num);
  cnt += sizeof(uint32_t);
  // write null bitmap
  uint32_t size = (fields_num + 7) / 8;
  char *bitmap = new char[size];
  for(uint32_t i = 0; i < fields_num; i++) {
    if(fields_[i]->IsNull()) {
      bitmap[i / 8] &= ~(1 << (7 - i % 8));
    }
    else {
      bitmap[i / 8] |= 1 << (7 - i % 8);
    }
  }
  memcpy(buf + cnt, bitmap, size);
  cnt += size;
  // write fields
  for(uint32_t i = 0; i < fields_num; i++) {
    if(!fields_[i]->IsNull()) {
      cnt += fields_[i]->SerializeTo(buf + cnt);
    }
  }
  return cnt;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t cnt = 0;
  uint32_t fields_num = MACH_READ_UINT32(buf + cnt);
  cnt += sizeof(uint32_t);
  // read null bitmap
  uint32_t size = (fields_num + 7) / 8;
  char *bitmap = new char[size];
  memcpy(bitmap, buf + cnt, size);
  cnt += size;
  // read fields
  for(uint32_t i = 0; i < fields_num; i++) {
    Field *field;
    if(bitmap[i / 8] & (1 << (7 - i % 8))) {
      cnt += field->Field::DeserializeFrom(buf + cnt, schema->GetColumn(i)->GetType(), &field, false);
    }
    else {
      cnt += field->Field::DeserializeFrom(buf + cnt, schema->GetColumn(i)->GetType(), &field, true);
    }

    fields_.push_back(field);
  }
  return cnt;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  size += sizeof(uint32_t);
  size += (fields_.size() + 7) / 8;
  for(uint32_t i = 0; i < fields_.size(); i++) {
    if(!fields_[i]->IsNull()) {
      size += fields_[i]->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
