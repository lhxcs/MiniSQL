#include "buffer/lru_replacer.h"
#include "glog/logging.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list_.size() == 0) {
    return false;
  }
  // LOG(INFO) << "victim's id " << *frame_id;
  for(auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
    *frame_id = *it;
  }
  lru_list_.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_list_.find(frame_id) != lru_list_.end()) {
    lru_list_.erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(lru_list_.find(frame_id) == lru_list_.end()) {
    lru_list_.insert(frame_id);
  }
  
  //LOG(INFO) << "current lru_list size " << lru_list.size();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}