#pragma once

#include <unordered_map>
#include <assert.h>

#include "common/config.h"
#include "rlib/rdma_ctrl.hpp"
#include "state_item/txn_state.h"
#include "state_item/common_use.h"

using namespace rdmaio;

/**
 * Remote node's ip&port info
*/
class RemoteNode {
public:
    int node_id;
    std::string ip;
    int port;
};

/**
 * MetaManager is used for MasterNode to manage the meta info in StateNode.
 * There are at least three replicas for a StateNode, 
 * the MasterNode has to write state info into three replicas
*/
class MetaManager {
public:
    static bool create_instance(const std::string& config_path);

    static void destroy_instance();

    static MetaManager* get_instance() {
        assert(global_meta_mgr != nullptr);
        return global_meta_mgr;
    }

    int GetMemStoreMeta(std::string& remote_ip, int remote_port);

    // 从remote node获取MemoryAttr信息，并保存
    void GetMRMeta(const RemoteNode& node);

    /**
     * Get the MemoryAttr of log_buffer in the specific remote StateNode
    */
    ALWAYS_INLINE
    const MemoryAttr& GetRemoteLogBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_log_buf_mrs.find(node_id);
        assert(mrsearch != remote_log_buf_mrs.end());
        return mrsearch->second;
    }

    /**
     * Get the MemoryAttr of lock_buffer in the specific remote StateNode
    */
    ALWAYS_INLINE
    const MemoryAttr& GetRemoteLockBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_lock_buf_mrs.find(node_id);
        assert(mrsearch != remote_lock_buf_mrs.end());
        return mrsearch->second;
    }

    /**
     * Get the MemoryAttr of the txn_list in the specific remote StateNode
    */
    ALWAYS_INLINE
    const MemoryAttr& GetRemoteTxnListMR(const node_id_t node_id) const {
        auto mrsearch = remote_txn_list_mrs.find(node_id);
        assert(mrsearch != remote_txn_list_mrs.end());
        return mrsearch->second;
    }

    ALWAYS_INLINE
    const MemoryAttr& GetRemoteSqlBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_sql_buf_mrs.find(node_id);
        assert(mrsearch != remote_sql_buf_mrs.end());
        return mrsearch->second;
    }

    ALWAYS_INLINE
    const MemoryAttr& GetRemotePlanBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_plan_buf_mrs.find(node_id);
        assert(mrsearch != remote_plan_buf_mrs.end());
        return mrsearch->second;
    }
    
    /*
        join state
    */
    ALWAYS_INLINE
    const MemoryAttr& GetRemoteJoinPlanBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_join_plan_buf_mrs.find(node_id);
        assert(mrsearch != remote_join_plan_buf_mrs.end());
        return mrsearch->second;
    }
    
    ALWAYS_INLINE
    const MemoryAttr& GetRemoteJoinBlockBufMR(const node_id_t node_id) const {
        auto mrsearch = remote_join_block_buf_mrs.find(node_id);
        assert(mrsearch != remote_join_block_buf_mrs.end());
        return mrsearch->second;
    }

    
    ALWAYS_INLINE
    node_id_t GetPrimaryNodeID() const {
        assert(remote_nodes.size() > 0);
        // the first remote_node is primary node, others are replicas
        return remote_nodes[0].node_id;
    }

    ALWAYS_INLINE
    offset_t GetTxnListLatchAddr() {
        assert(txn_list_latch_addr != -1);
        return txn_list_latch_addr;
    }

    ALWAYS_INLINE
    offset_t GetTxnListBitmapAddr() {
        // assert(txn_list_bitmap_addr != -1);
        // return txn_list_bitmap_addr;
    }

    ALWAYS_INLINE
    offset_t GetTxnAddrByIndex(int index) {
        assert(txn_list_base_addr != -1);
        assert(txn_size != -1);
        assert(index != -1);
        // assert(txn_list_bitmap_ != nullptr);
        // std::cout << "index: " << index << ", bitmap_size: " << txn_list_bitmap_->bitmap_size_ << "\n";
        // assert(index < txn_list_bitmap_->bitmap_size_ * 8);
        return txn_list_base_addr + index * txn_size;
    }

    ALWAYS_INLINE
    offset_t GetSqlBufAddrByThread(int thread_id) {
        return thread_id * THREAD_LOCAL_SQLBUF_SIZE;
    }

    ALWAYS_INLINE
    offset_t GetPlanBufAddrByThread(int thread_id) {
        return thread_id * THREAD_LOCAL_PLANBUF_SIZE;
    }

    ALWAYS_INLINE
    offset_t GetCursorBufAddrByThread(int thread_id) {
        return thread_id * THREAD_LOCAL_CURSORBUF_SIZE;
    }

    /*
        remote join plan && block addr
    */
    ALWAYS_INLINE
    offset_t GetJoinPlanAddrByThread(int thread_id) {
        return thread_id * PER_THREAD_JOIN_PLAN_SIZE;
    }

    ALWAYS_INLINE
    offset_t GetJoinBlockAddrByThread(int thread_id) {
        return thread_id * PER_THREAD_JOIN_BLOCK_SIZE;
    }

    // ALWAYS_INLINE
    // size_t GetTxnBitmapSize() {
    //     return txn_bitmap_size;
    // }

private:
    MetaManager(const std::string& config_path);
    ~MetaManager() {}

    static MetaManager* global_meta_mgr;

    node_id_t local_machine_id;
    // MemoryAttrs for various states in remote StateNodes, there may be multiple StateNodes because we assume that the StateNode can elasticly expand.
    std::unordered_map<node_id_t, MemoryAttr> remote_lock_buf_mrs;          // MemoryAttr for lock_buffer in remote StateNodes
    std::unordered_map<node_id_t, MemoryAttr> remote_txn_list_mrs;          // MemoryAttr for txn_list in remote StateNodes
    std::unordered_map<node_id_t, MemoryAttr> remote_log_buf_mrs;           // MemoryAttr for log_buffer in remote StateNodes
    std::unordered_map<node_id_t, MemoryAttr> remote_sql_buf_mrs;           // MemoryAttr for query_buf in remote StateNodes
    std::unordered_map<node_id_t, MemoryAttr> remote_plan_buf_mrs;          // MemoryAttr for plan_buf in remote StateNodes
    
    /* 
        join executor内部状态记录
        1. join plan
        2. join block
    */
    std::unordered_map<node_id_t, MemoryAttr> remote_join_plan_buf_mrs;     // MemoryAttr for join_plan_buf in remote StateNodes
    std::unordered_map<node_id_t, MemoryAttr> remote_join_block_buf_mrs;    // MemoryAttr for join_plan_buf in remote StateNodes

    // meta info for txn_list
    // offset_t txn_list_latch_addr = 0;   // base address for txn_list_latch
    // offset_t txn_list_bitmap_addr = sizeof(rwlatch_t);  // base address for txn_list bitmap
    // offset_t txn_list_base_addr = sizeof(rwlatch_t) + txn_list_bitmap_->bitmap_size_;    // base address for txn_list
    // size_t txn_size = sizeof(TxnItem);                // size for each txn_item in txn_list
    // size_t txn_bitmap_size = MAX_CONN_LIMIT;            // size for txn_list bitmap, initiated 
    offset_t txn_list_latch_addr = -1;   // base address for txn_list_latch
    // offset_t txn_list_bitmap_addr = -1;  // base address for txn_list bitmap
    offset_t txn_list_base_addr = -1;    // base address for txn_list
    size_t txn_size = -1;                // size for each txn_item in txn_list

public:
    RNicHandler* opened_rnic = nullptr;
    RdmaCtrlPtr global_rdma_ctrl;           // rdma controller used by QPManager and local RDMA Region
    std::vector<RemoteNode> remote_nodes;   // remote state nodes
    // RegionBitmap* txn_list_bitmap_ = nullptr;        // txn_list bitmap
};