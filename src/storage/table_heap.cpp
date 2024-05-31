#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  if(row.GetSerializedSize(schema_) >= PAGE_SIZE) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if(page == nullptr) return false;
  page->WLatch();
  while(!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next_id = page->GetNextPageId();
    if(next_id == INVALID_PAGE_ID){
      page_id_t new_page_id;
      buffer_pool_manager_->NewPage(new_page_id);
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id));
      new_page->Init(new_page_id, page->GetPageId(), log_manager_, txn);
      new_page->WLatch();
      new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page_id, true);///////
      page->SetNextPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);///////
      return true;
    }
    else{
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));
      page->WLatch();
    }
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (old_page == nullptr) {
    return false;
  }
  old_page->WLatch();
  Row old_row(rid);
  bool result = old_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(old_page->GetTablePageId(), true);
  return result;

 }

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page!= nullptr,"page not found.");
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->RLatch();
  bool result = page->GetTuple(row, schema_, txn,lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return result;
 }

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId row_id;
  page->RLatch();
  page->GetFirstTupleRid(&row_id);
  page->RUnlatch();
  return TableIterator(this, row_id, txn);
 }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  return TableIterator(this,INVALID_ROWID,nullptr);
 }
