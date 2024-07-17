#pragma once

#include <vector>
#include <algorithm>
#include <set>

class RegionManager {
public:
    RegionManager(size_t totalSize) : totalSize_(totalSize) {
        freeRegions_.insert(std::make_pair(0, totalSize));
    }

    // 分配空间
    void* allocate(size_t size) {
        for (auto it = freeRegions_.begin(); it != freeRegions_.end(); ++it) {
            if (it->second - it->first >= size) {
                void* allocatedPtr = reinterpret_cast<void*>(it->first);
                allocatedRegions_.emplace_back(it->first, it->first + size);
                // 删除旧的空闲区间
                auto oldIt = it;
                ++it;
                freeRegions_.erase(oldIt);
                // 插入新的空闲区间
                if (it != freeRegions_.end()) {
                    it = freeRegions_.insert({it->first + size, it->second}).first;
                } else {
                    freeRegions_.insert({it->first + size, totalSize_});
                }
                return allocatedPtr;
            }
        }
        return nullptr; // 没有足够的连续空间分配
    }

    // 释放空间
    void free(void* ptr) {
        auto it = std::find_if(allocatedRegions_.begin(), allocatedRegions_.end(), 
                                [ptr](const std::pair<size_t, size_t>& region) {
                                    return ptr >= reinterpret_cast<void*>(region.first) &&
                                           ptr < reinterpret_cast<void*>(region.second);
                                });
        if (it != allocatedRegions_.end()) {
            // 释放空间并进行空闲空间的合并
            size_t start = it->first;
            size_t end = it->second;
            allocatedRegions_.erase(it);
            auto mergeIt = freeRegions_.upper_bound(std::make_pair(start, end));
            if (mergeIt != freeRegions_.begin() && std::prev(mergeIt)->second == start) {
                start = std::prev(mergeIt)->first;
                freeRegions_.erase(std::prev(mergeIt));
            }
            if (mergeIt != freeRegions_.end() && mergeIt->first == end) {
                end = mergeIt->second;
                freeRegions_.erase(mergeIt);
            }
            freeRegions_.emplace(start, end);
        }
    }

private:
    size_t totalSize_;
    std::vector<std::pair<size_t, size_t>> allocatedRegions_; // 已分配的空间
    std::set<std::pair<size_t, size_t>> freeRegions_; // 空闲的连续空间
};