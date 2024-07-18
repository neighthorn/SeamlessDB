#pragma once

#include "state/state_item/txn_state.h"

// from right to left, 0-->bitmap_size-1
static int GetFirstFreeBit(unsigned char* bitmap, size_t bitmap_size) {
    for(int i = 0; i < bitmap_size; ++i) {
        if (static_cast<unsigned char>(bitmap[i]) != 0xFF) {
            int offset = 0;
            while((bitmap[i] & (1 << offset)) != 0) {
                ++offset;
            }
            return i * 8 + offset;
        }
    }
    return -1;
}

// change the specific bit from 0 to 1
static void SetBitToUsed(unsigned char* bitmap, int pos) {
    bitmap[pos / 8] |= (1 << (pos % 8));
}

// change the specific bit from 1 to 0
static void SetBitToFree(unsigned char* bitmap, int pos) {
    bitmap[pos / 8] &= ~(1 << (pos % 8));
}

static uint64_t HashFunc(const void* data, size_t size) {
    const uint64_t fnv_prime = 1099511628211ULL;
    uint64_t hash = 14695981039346656037ULL;

    const uint8_t* byte_data = static_cast<const uint8_t*>(data);

    for(size_t i = 0; i < size; ++i) {
        hash ^= byte_data[i];
        hash *= fnv_prime;
    }

    return hash;
}

// static void GetHashCodeForTxn(TxnItem* txn) {
//     txn->hash_code ^= HashFunc(&txn->id, sizeof(txn_id_t));
//     txn->hash_code ^= HashFunc(&txn->txn_mode, sizeof(bool));
//     txn->hash_code ^= HashFunc(&txn->thread_id, sizeof(t_id_t));
//     txn->hash_code ^= HashFunc(&txn->txn_state, sizeof(TransactionState));
// }

static std::vector<int> GetAllUsedBit(unsigned char* bitmap,size_t bitmap_size){
    std::vector<int> res;
    for (int i = 0; i < bitmap_size; i++){
        if (static_cast<unsigned char>(bitmap[i])!=0){
            for(int offset=0;offset<8;offset++){
                if ((bitmap[i] & (1 << offset)) != 0){
                    res.push_back(i*8+offset);
                }
            }
        }
    }
    return res;
}

// static std::vector<TxnItem*> GetTxnItemsFromRemote(THD* thd){
//     std::vector<TxnItem*> res;
//     node_id_t primary_node_id = MetaManager::get_instance()->GetPrimaryNodeID();
//     RCQP* qp = thd->qp_manager->GetRemoteTxnListQPWithNodeID(primary_node_id);
//     MetaManager* meta_mgr = MetaManager::get_instance();

//     char* bitmap_latch_buf = thd->rdma_buffer_allocator->Alloc(sizeof(rwlatch_t));
//     *(rwlatch_t*)bitmap_latch_buf = BITMAP_LOCKED;
    
//     // get latch for txn_list_bitmap
//     // while(*(rwlatch_t*)bitmap_latch_buf != BITMAP_UNLOCKED) {
//     //     if(!thd->coro_sched->RDMACASSync(0, qp, bitmap_latch_buf, meta_mgr->GetTxnListLatchAddr(), (uint64_t)BITMAP_UNLOCKED, (uint64_t)BITMAP_LOCKED)){
//     //         return  res;
//     //     }
//     // }

//     // read txn_list_bitmap
//     size_t txn_bitmap_size = meta_mgr->GetTxnBitmapSize();
//     unsigned char* txn_list_bitmap = (unsigned char*)thd->rdma_buffer_allocator->Alloc(txn_bitmap_size);
//     if(!thd->coro_sched->RDMAReadSync(0, qp, (char*)txn_list_bitmap, meta_mgr->GetTxnListBitmapAddr(), txn_bitmap_size)){
//         return res;
//     }

//     std::vector<int> TxnusedIndex=GetAllUsedBit(txn_list_bitmap,txn_bitmap_size);
//     for(int txnindex:TxnusedIndex){
//         TxnItem* txn_item_buf = (TxnItem*)thd->rdma_buffer_allocator->Alloc(sizeof(TxnItem));
//         if(!thd->coro_sched->RDMAReadSync(0, qp, (char*)txn_item_buf, meta_mgr->GetTxnAddrByIndex(txnindex), sizeof(TxnItem))) {
//             std::cout<<"read txn info error"<<std::endl;
//             return res;
//         }
//         res.push_back(txn_item_buf);
//     }
    
//     return res;
// }