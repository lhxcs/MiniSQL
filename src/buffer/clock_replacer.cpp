#include "buffer/clock_replacer.h"
#include "glog/logging.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
    for(size_t i = 0; i < num_pages; i++){
        clock_status.push_back({true,false});
    }
    clock_pointer = 0;
    capacity = 0;
};
CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if(capacity == 0) {
        return false;
    }
    while(true) {
        if(clock_pointer == clock_status.size()){
            clock_pointer = 0;
        }
        if(clock_status[clock_pointer].first == true) {
            clock_pointer++;
        } else {
            if(clock_status[clock_pointer].second == true) {
            clock_status[clock_pointer].second = false;
            clock_pointer++;
            } else {
                *frame_id = clock_pointer;
                clock_status[clock_pointer].first = true;
                capacity--;
                return true;
            }
        }
    }
    //LOG(INFO) << "Victim " << *frame_id << " current size " << CLOCKReplacer::Size();
   
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    if(clock_status[frame_id].first == false) {
        clock_status[frame_id].first = true;
        capacity--;
    }
    //LOG(INFO) <<"Pin "<< frame_id<< " current size " << CLOCKReplacer::Size();
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    if(clock_status[frame_id].first == true) {
        clock_status[frame_id].first = false;
        clock_status[frame_id].second = true;
        capacity++;
    }
    //LOG(INFO) << "Unpin" << frame_id<< " current size " << CLOCKReplacer::Size();
}

size_t CLOCKReplacer::Size() {
    return capacity;
} 