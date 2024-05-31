#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  row_ = new Row(rid);
  if (rid.GetPageId() != INVALID_PAGE_ID) {
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

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->row_ == itr.row_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return this->row_ != itr.row_;
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->table_heap_ = itr.table_heap_;
  this->row_ = itr.row_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (row_ == nullptr || row_->GetRowId() == INVALID_ROWID) {
    return *this;
  }
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  RowId next_rid;
  page->RLatch();
  if (!page->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
    while(page->GetNextPageId() != INVALID_PAGE_ID) {
      page_id_t next_page_id = page->GetNextPageId();
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      page->RLatch();
      if (page->GetFirstTupleRid(&next_rid)) {
        break;
      }
    }
    if(page->GetNextPageId() == INVALID_PAGE_ID) {
      delete row_;
      row_ = new Row(INVALID_ROWID);
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return *this;
    }
  }
  page->RUnlatch();
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  delete row_;
  row_ = new Row(next_rid);
  table_heap_->GetTuple(row_, nullptr);
  return *this;

}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator p(table_heap_, row_->GetRowId(), nullptr);
  ++(*this);
  return TableIterator{p};
}
