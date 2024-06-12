#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //ASSERT(false, "Not Implemented yet");
  return 3 * sizeof(uint32_t) + table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
      index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  if(init) {
    this->catalog_meta_ = CatalogMeta::NewInstance();
    this->next_index_id_ = catalog_meta_->GetNextIndexId();
    this->next_table_id_ = catalog_meta_->GetNextTableId();
  } else {
    Page *page = this->buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    this->catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    this->next_index_id_ = catalog_meta_->GetNextIndexId();
    this->next_table_id_ = catalog_meta_->GetNextTableId();
    for(auto iter : catalog_meta_->table_meta_pages_) {
      LoadTable(iter.first, iter.second);
    }
    for(auto iter : catalog_meta_->index_meta_pages_) {
      LoadIndex(iter.first, iter.second);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  }
  FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // //ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  TableSchema * schema_copy = Schema::DeepCopySchema(schema);

  //初始化 table 信息
  table_id_t table_id = next_table_id_;
  next_table_id_++;
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema_copy, txn, log_manager_, lock_manager_);
  //LOG(INFO) << "create table heap" << endl;
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema_copy);
  //LOG(INFO) << "create tabel meta" << endl;
  table_info = TableInfo::Create();
  //LOG(INFO) << "create table info" << endl;
  table_info->Init(table_meta, table_heap);
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  //新建数据页,并将table 序列化到这个数据页中
  //LOG(INFO) << "here" << endl;
  page_id_t page_id;
  Page* table_meta_page = buffer_pool_manager_->NewPage(page_id);
  table_meta->SerializeTo(table_meta_page->GetData()); //Fail
  //LOG(INFO) << "SerializeTo"<<endl;
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //LOG(INFO) <<"Running gettable"<<endl;
  auto iter = table_names_.find(table_name);
  if(iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  GetTable(iter->second, table_info);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  index_id_t index_id = next_index_id_;
  next_index_id_++;
  table_id_t table_id = table_names_[table_name];
  TableInfo* table_info = tables_[table_id];
  uint32_t column_index;
  std::vector<uint32_t> key_map;
  for(const auto &iter: index_keys) {
    if(table_info->GetSchema()->GetColumnIndex(iter, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(column_index);
  }

  index_info = IndexInfo::Create();
  IndexMetadata *index_meta_data = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_info->Init(index_meta_data, table_info, buffer_pool_manager_);

  auto itr = table_info->GetTableHeap()->Begin(txn);
  vector<u_int32_t> column_idx;
  vector<Column *> columns = index_info->GetIndexKeySchema()->GetColumns();
  for(auto column: columns) {
    uint32_t column_id;
    if(table_info->GetSchema()->GetColumnIndex(column->GetName(), column_id) == DB_SUCCESS) {
      column_idx.push_back(column_id);
    }
  }
  for(; itr != table_info->GetTableHeap()->End(); itr++) {
    Row tmp = *itr;
    vector<Field> fields;
    for(auto idx: column_idx) {
      fields.push_back(*tmp.GetField(idx));
    }
    Row index_row(fields);
    index_info->GetIndex()->InsertEntry(index_row, tmp.GetRowId(), txn);
  }
  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  index_meta_data->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  catalog_meta_->index_meta_pages_[index_id] = page_id;
  indexes_.emplace(index_id, index_info);
  index_names_[table_name].emplace(index_name, index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto iter_table_name = index_names_.find(table_name);
  if(iter_table_name == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto iter_index_name = iter_table_name->second.find(index_name);
  if(iter_index_name == iter_table_name->second.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = iter_index_name->second;
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
 }

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto iter_table_name = index_names_.find(table_name);
  if(iter_table_name == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  for(auto iter : iter_table_name->second) {
    indexes.push_back(indexes_.at(iter.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  //ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo* table_info = tables_[table_id];
  if(table_info == nullptr) {
    return DB_FAILED;
  } 
  tables_.erase(table_id);
  table_names_.erase(table_name);
  table_info->~TableInfo();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  //ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto it = index_names_[table_name].find(index_name);
  if(it == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }
  IndexInfo* index_info = indexes_[it->second];
  catalog_meta_->index_meta_pages_.erase(it->second);
  indexes_.erase(it->second);
  index_names_[table_name].erase(index_name);
  index_info->GetIndex()->Destroy();
  index_info->~IndexInfo();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  Page* page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;
  table_names_[table_meta->GetTableName()] = table_id;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  IndexInfo* index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  index_names_[tables_[index_meta->GetTableId()]->GetTableName()][index_meta->GetIndexName()] = index_id;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto iter = tables_.find(table_id);
  if(iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = iter->second;
  return DB_SUCCESS;
}
