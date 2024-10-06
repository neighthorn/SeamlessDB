#pragma once

#include <vector>

#include "common/config.h"

// bitmap used to manage active transaction list/locks in state node
// we update the bitmap in compute pool and flush the bitmap into the state node with the updated transaction/lock info simutaneously
struct RegionBitmap {
    RegionBitmap(size_t bitmap_size, int thread_num): bitmap_size_((bitmap_size + 7) / 8) {
        bitmap_ = new unsigned char[bitmap_size_];
        memset(bitmap_, 0, bitmap_size_);
        thread_local_size_ = bitmap_size_ / thread_num;
        // std::cout << "bitmap_size: " << bitmap_size_ << "\n";
    }
    ~RegionBitmap() {
        if(bitmap_) delete[] bitmap_;
    }

    int get_first_free_bit(int thread_index) {
        // std::cout << "get first free bit\n";
        int begin = thread_local_size_ * thread_index;
        int end = thread_local_size_ * (thread_index + 1);
        // std::lock_guard<std::mutex> lock(latch_);
        // std::cout << "got latch\n";
        for(int i = begin; i < end; ++i) {
            if ((bitmap_[i]) != 0xFF) {
                int offset = 0;
                while((bitmap_[i] & (1 << offset)) != 0) {
                    ++offset;
                }
                bitmap_[i] |= (1 << offset);
                return i * 8 + offset;
            }
        }
        return -1;
    }

    void set_bit_to_free(int pos) {
        // std::cout << "set bit to free\n";
        // std::lock_guard<std::mutex> lock(latch_);
        bitmap_[pos / 8] &= ~(1 << (pos % 8));
    }

    void get_curr_bitmap(char* dest) {
        // std::cout << "get curr bitmap\n";
        std::lock_guard<std::mutex> lock(latch_);
        memcpy(dest, (char*)bitmap_, bitmap_size_);
    }

    // used for test
    std::vector<int> get_all_valid_bits(char* src, int size) {
        std::vector<int> valid_bits;
        for(int i = 0; i < size; ++i) 
            for(int bit = 0; bit < 8; ++bit) {
                if(src[i] & ( 1 << bit))
                    valid_bits.push_back(i * 8 + bit);
            }
        return valid_bits;
    }

    std::mutex latch_;
    unsigned char* bitmap_;
    size_t bitmap_size_;
    int thread_local_size_;
};

