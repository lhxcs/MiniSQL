#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_heap_ = table_heap;
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    row_ = new Row(rid);
    table_heap_->GetTuple(row_, txn);
  } 
  else {
    row_ = new Row(INVALID_ROWID);
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->row_->GetRowId() == itr.row_->GetRowId() && this->table_heap_ == itr.table_heap_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !this->operator==(itr);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->table_heap_=itr.table_heap_;
  this->row_=new Row(*itr.row_);
  return *this;
}

//++iter
TableIterator &TableIterator::operator++() {
  if (row_ == nullptr || row_->GetRowId().GetPageId() == INVALID_PAGE_ID) {
    return *this;
  }
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  RowId next_rid;
  page->RLatch();
  if (!page->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
    page->RUnlatch();
    if(page->GetNextPageId() != INVALID_PAGE_ID) {
      page_id_t next_page_id = page->GetNextPageId();
      
      auto next_page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      next_page->RLatch();
      next_page->GetFirstTupleRid(&next_rid);
      next_page->RUnlatch();
    }
    else {
      next_rid = INVALID_ROWID;
    }
  }
  else {
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  delete row_;
  row_ = new Row(next_rid);
  if(next_rid.GetPageId()!=INVALID_PAGE_ID) {
    table_heap_->GetTuple(row_, nullptr);
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  RowId rid=this->row_->GetRowId();
  this->operator++();
  return TableIterator(this->table_heap_,rid,nullptr);
}
