#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ >= this->GetMaxSupportedSize()) {
    return false;
  }
  page_offset = next_free_page_;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] |= (1 << bit_index);
  page_allocated_++;
  for(uint32_t i = 0; i < this->GetMaxSupportedSize(); i++) {
    byte_index = i / 8;
    bit_index = i % 8;
    if(IsPageFreeLow(byte_index, bit_index)) {
      next_free_page_ = i;
      break;
    }
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset >= this->GetMaxSupportedSize()) {
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  
  if((IsPageFreeLow(byte_index, bit_index))) {
    return false;
  }
  bytes[byte_index] &= ~(1 << bit_index);
  page_allocated_--;
  if(page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset >= this->GetMaxSupportedSize()) {
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;