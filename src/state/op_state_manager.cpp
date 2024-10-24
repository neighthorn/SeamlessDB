#include <iostream>

#include "op_state_manager.h"
#include "state_item/op_state.h"
#include "execution/executor_abstract.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"
#include "execution/executor_projection.h"
#include "execution/execution_sort.h"

#include "debug_log.h"

/*
    初始化 write cnts;
*/
int OperatorStateManager::write_cnts = 0;
int64_t OperatorStateManager::write_tot_size = 0;
int OperatorStateManager::add_cktp_cnts = 0;

OperatorStateManager::OperatorStateManager(int connection_id, CoroutineScheduler *coro_sched, MetaManager *meta_manager, QPManager *qp_manager) : connection_id_(connection_id),
    coro_sched_(coro_sched), meta_manager_(meta_manager), qp_manager_(qp_manager)
{

    primary_node_id_ = meta_manager_->GetPrimaryNodeID();

    local_sql_region_            = RDMARegionAllocator::get_instance()->GetThreadLocalSQLRegion(connection_id);
    local_plan_region_           = RDMARegionAllocator::get_instance()->GetThreadLocalJoinPlanRegion(connection_id);
    local_op_checkpoint_region_  = RDMARegionAllocator::get_instance()->GetThreadLocalJoinBlockRegion(connection_id);
    local_op_checkpoint_read_cache_region_ = RDMARegionAllocator::get_instance()->GetThreadLocalOPCheckpointReadCacheRegion(connection_id);

    sql_buffer_allocator_     = new RDMABufferAllocator(local_sql_region_.first, local_sql_region_.second);
    plan_buffer_allocator_    = new RDMABufferAllocator(local_plan_region_.first, local_plan_region_.second);
    op_checkpoint_meta_buffer_  = local_op_checkpoint_region_.first;
    op_checkpoint_buffer_allocator_ = new RDMABufferAlloc(local_op_checkpoint_region_.first + CheckPointMetaSize, local_op_checkpoint_region_.second);
    op_checkpoint_read_cache_allocator_ = new RDMABufferAllocator(local_op_checkpoint_read_cache_region_.first, local_op_checkpoint_read_cache_region_.second);

    sql_qp_           = qp_manager_->GetRemoteSqlBufQPWithNodeID(primary_node_id_);
    plan_qp_          = qp_manager_->GetRemoteJoinPlanBufQPWithNodeID(primary_node_id_);
    op_checkpoint_qp_ = qp_manager_->GetRemoteJoinBlockBufQPWithNodeID(primary_node_id_);

    ck_meta_ = std::make_unique<CheckPointMeta>();
    ck_meta_->thread_id = coro_sched_->t_id_;
    ck_meta_->checkpoint_num = 0;
    ck_meta_->total_size = 0;

    total_src_op = 0;
    
    /*
        next write offset
    */
    op_next_write_offset_ = CheckPointMetaSize;

    op_checkpoint_write_thread_ = new std::thread(&OperatorStateManager::write_op_state_thread, this);
}

bool OperatorStateManager::finish_write() {
    return write_cnts == ck_meta_->checkpoint_num;
}

/*
    初始化remote state node的内容
*/
void OperatorStateManager::reset_remote_sql_and_op_region() {
    /*
        初始化sql state
    */
    write_sql_to_state(-1, nullptr, 0);
    /*
        初始化plan state
    */

    /*
        checkpoint meta
    */

    std::unique_lock<std::mutex> op_meta_lock(op_meta_latch_);

    ck_meta_ = std::make_unique<CheckPointMeta>();
    ck_meta_->thread_id = coro_sched_->t_id_;
    ck_meta_->checkpoint_num = 0;
    ck_meta_->total_size = 0;

    write_op_checkpoint_meta();
}

/*

*/
void OperatorStateManager::write_sql_to_state(int sql_id, char* sql, int len) {
    assert(sql_qp_ != nullptr);

    /*
        construct sql state
    */
    SQLState sql_state = SQLState::construct_sql_state(sql_id, sql, len);
    size_t sql_state_size = sql_state.cal_size();

    /*
        alloc buffer
    */
    char*   sql_buf = sql_buffer_allocator_->Alloc(sql_state_size);
    int     remote_offset  = meta_manager_->GetSqlBufAddrByThread(coro_sched_->t_id_);

    // memcpy(sql_buf, sql, len);
    /*
        sql state serialize
    */
    size_t acutal_size = sql_state.serialize(sql_buf);
    if(!coro_sched_->RDMAWriteSync(0, sql_qp_, sql_buf, remote_offset, acutal_size)) {
        RDMA_LOG(ERROR) << "Failed to write sql into state_node.";
        assert(0);
    }

    // RwServerDebug::getInstance()->DEBUG_PRINT("[WRITE SQL][THREAD ID: " + std::to_string(coro_sched_->t_id_)  + "][SQLID: " + std::to_string(sql_state.sql_id) + "][SQL SIZE: " + std::to_string(sql_state.sql_size) + "][SQL: " + sql_state.sql + "] write sql to state node.");
}

std::unique_ptr<SQLState> OperatorStateManager::read_sql_from_state() {
    assert(sql_qp_ != nullptr);

    /*
        get remote offset
    */
    size_t remote_offset = meta_manager_->GetSqlBufAddrByThread(coro_sched_->t_id_);

    /*
        alloc local rdma memory
    */
    char *sql_state_buf = sql_buffer_allocator_->Alloc(SQLState::SQL_STATE_MAX_SIZE);

    /*
        rdma read
    */
    if(!coro_sched_->RDMAReadSync(0, sql_qp_, sql_state_buf, remote_offset, SQLState::SQL_STATE_MAX_SIZE)) {
        RDMA_LOG(ERROR) << "Failed to read sql from state_node.";
        assert(0);
    }

    auto sql_state = std::make_unique<SQLState>();
    bool status = sql_state->deserialize(sql_state_buf);
    if(!status) {
        std::cerr << "sql state deserialize failed!\n";
    }

    // RwServerDebug::getInstance()->DEBUG_PRINT("[READ SQL][THREAD ID: " + std::to_string(coro_sched_->t_id_) + "][SQLID: " + std::to_string(sql_state->sql_id) + "][SQL SIZE: " + std::to_string(sql_state->sql_size) + "][SQL: " + sql_state->sql + "] read sql from state node.");
    return std::move(sql_state);
}



void OperatorStateManager::write_plan_to_state(int sql_id, SmManager* sm_mgr, std::shared_ptr<Plan> plan) {
    assert(plan_qp_ != nullptr);

    /*
        将plan序列化到plan_buf中
    */
    char    *plan_buf = nullptr;
    int     plan_node_count = 0;
    int     tot_size  = 0;
    size_t  remote_offset = 0;

    if(auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {

    } else if(auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {

    } else if(auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
        switch(x->tag) {
            case T_select: {

                /*
                    将plan序列化到plan_buf中
                */

                plan_node_count = plan->plan_tree_size();
                plan_buf = plan_buffer_allocator_->Alloc(plan_node_count * STATE_SIZE_PER_PLAN);
                tot_size = plan->serialize(plan_buf);
                #ifdef PRINT_LOG
                    // RwServerDebug::getInstance()->DEBUG_WRITE_PLAN(plan_node_count, tot_size, sql);
                    // std::cout << "plan_node_count: " << plan_node_count << ", tot_size: " << tot_size << "\n";
                #endif

                /*
                    get remote offset of state node
                */
                remote_offset = meta_manager_->GetJoinPlanAddrByThread(coro_sched_->t_id_);

                /*
                    RMDA write
                */
                if(!coro_sched_->RDMAWriteSync(0, plan_qp_, plan_buf, remote_offset, tot_size)) {
                    RDMA_LOG(ERROR) << "Failed to write plan into state_node.";
                    assert(0);
                }

            } break;
            default:
                std::cout << "Not impleted!\n";
            break;
            
        }
    } else {

    }
    
    // RwServerDebug::getInstance()->DEBUG_PRINT("[WRITE PLAN][SQLID: " + std::to_string(sql_id) + "] write plan to state node.");
        /*
            TEST
            read plan from state and deserialize
        */

        // if(plan_buf == nullptr) {
        //     return ;
        // }
        // // TEST
        // memset(plan_buf, 0, tot_size);
        // coro_sched_->RDMAReadSync(0, plan_qp_, plan_buf, remote_offset, tot_size);

        // PlanTag root_plan_tag = *reinterpret_cast<const PlanTag*>(plan_buf + sizeof(int));
        // std::shared_ptr<Plan> read_plan;
        // switch(root_plan_tag) {
        //     case PlanTag::T_select: {
        //         read_plan = DMLPlan::deserialize(plan_buf, sm_mgr);
        //     } break;
        //     case PlanTag::T_Update:
        //     case PlanTag::T_Delete:
        //     case PlanTag::T_Insert: 
        //     case PlanTag::T_Help:
        //     case PlanTag::T_ShowTable:
        //     case PlanTag::T_DescTable:
        //     case PlanTag::T_Transaction_abort:
        //     case PlanTag::T_Transaction_begin:
        //     case PlanTag::T_Transaction_commit:
        //     case PlanTag::T_Transaction_rollback:
        //     case PlanTag::T_CreateTable: 
        //     case PlanTag::T_DropTable:
        //     case PlanTag::T_CreateIndex:
        //     case PlanTag::T_DropIndex:
        //     break;
        // }

        // @TEST
        // memset(plan_buf, 0, tot_size);
        // context->coro_sched_->RDMAReadSync(0, qp, plan_buf, offset, tot_size);
        // PlanTag root_plan_tag = *reinterpret_cast<const PlanTag*>(plan_buf + sizeof(int));
        // std::shared_ptr<Plan> read_plan;
        // switch(root_plan_tag) {
        //     case PlanTag::T_select:
        //     case PlanTag::T_Update:
        //     case PlanTag::T_Delete:
        //     case PlanTag::T_Insert: {
        //         read_plan = DMLPlan::deserialize(plan_buf, sm_mgr);
        //     } break;
        //     case PlanTag::T_Help:
        //     case PlanTag::T_ShowTable:
        //     case PlanTag::T_DescTable:
        //     case PlanTag::T_Transaction_abort:
        //     case PlanTag::T_Transaction_begin:
        //     case PlanTag::T_Transaction_commit:
        //     case PlanTag::T_Transaction_rollback: {
        //         read_plan = OtherPlan::deserialize(plan_buf, sm_mgr);
        //     } break;
        //     case PlanTag::T_CreateTable: 
        //     case PlanTag::T_DropTable:
        //     case PlanTag::T_CreateIndex:
        //     case PlanTag::T_DropIndex: {
        //         read_plan = DDLPlan::deserialize(plan_buf, sm_mgr);
        //     } break;
        //     default: {
        //         std::cout << "Invalid plan tag.\n";
        //     } break;
        // }
}


std::pair<bool, size_t> OperatorStateManager::add_operator_state_to_buffer(AbstractExecutor *abstract_executor, double src_op) {
    if(write_ckpt_ == false) return {true, 0};
    std::unique_lock<std::mutex> lock(op_latch_);

    /*
        write_status    操作是否成功
        actual size     实际写入大小
    */
    bool    write_status = false;
    size_t  actual_size = 0;

    ck_meta_->total_src_op += src_op;
    total_src_op += src_op;

    if(auto scan_op = dynamic_cast<IndexScanExecutor *>(abstract_executor)) {
        // scan operator
        
        /*
            构造scan state
        */
        IndexScanOperatorState index_scan_state(scan_op);
        size_t index_scan_state_size = index_scan_state.getSize();

        // if(add_cktp_cnts == 13) std::cout << "IndexScanOperatorState size: " << index_scan_state_size << std::endl;

        /*
            分配buffer
        */
        char *alloc_buffer;
        do{
            auto [status, buffer] = op_checkpoint_buffer_allocator_->Alloc(index_scan_state_size);
            if(status) {
                alloc_buffer = buffer;
                
                break;
            } else {
                std::cout << "waiting for free buffer.\n";
                op_checkpoint_not_full_.wait(lock);
            }
        }while(true);
        
        /*
            serialize
        */
        actual_size = index_scan_state.serialize(alloc_buffer);
        assert(actual_size == index_scan_state_size);
        write_status = true;

        /*
            add to checkpoint queue
        */
        op_checkpoint_queue_.push(OpCheckpointBlock{.buffer = alloc_buffer, .size = actual_size});

        // RwServerDebug::getInstance()->DEBUG_PRINT("[PUSH STATE][SIZE " + std::to_string(actual_size) + "] push state to local memory");
        /*
            notify
        */
        op_checkpoint_not_empty_.notify_all();

    } else if(auto block_join_op = dynamic_cast<BlockNestedLoopJoinExecutor *>(abstract_executor)) {
        /*
            构造block state
        */
        BlockJoinOperatorState block_join_state(block_join_op);
        size_t block_join_checkpoint_size = block_join_state.getSize();

        // if(add_cktp_cnts == 13) std::cout << "BlockJoinOperatorState size: " << block_join_checkpoint_size << std::endl;

        /*
            分配buffer
        */

        char *alloc_buffer;
        do{
            auto [status, buffer] = op_checkpoint_buffer_allocator_->Alloc(block_join_checkpoint_size);

            if(status) {
                alloc_buffer = buffer;
                break;
            } else {
                std::cout << "waiting for free buffer.\n";
                std::cout << "Used space: " << op_checkpoint_buffer_allocator_->getUsedSpace() << ", free space: " << op_checkpoint_buffer_allocator_->getFreeSpace() << std::endl;
                std::cout << "current write offset: " << op_checkpoint_buffer_allocator_->get_curr_write_offset() << ", current read offset: " << op_checkpoint_buffer_allocator_->get_curr_read_offset() << std::endl;
                op_checkpoint_not_full_.wait(lock);
            }
        }while(true);

        /*
            serialize
        */
        actual_size = block_join_state.serialize(alloc_buffer);
        assert(actual_size == block_join_checkpoint_size);
        write_status = true;
        /*
            add to checkpoint queue
        */
        op_checkpoint_queue_.push(OpCheckpointBlock{.buffer = alloc_buffer, .size = actual_size});

        // RwServerDebug::getInstance()->DEBUG_PRINT("[PUSH STATE][SIZE " + std::to_string(actual_size) + "] push state to local memory");
        /*
            notify
        */
        op_checkpoint_not_empty_.notify_all();

    } else if(auto hash_join_op = dynamic_cast<HashJoinExecutor *>(abstract_executor)) {
        HashJoinOperatorState hash_join_state(hash_join_op);
        size_t hash_join_checkpoint_size = hash_join_state.getSize();

        // if(add_cktp_cnts == 13) {
        //     std::cout << "HashJoinOperatorState size: " << hash_join_checkpoint_size << std::endl;
            
        // }

        char* alloc_buffer;
        do {
            auto [status, buffer] = op_checkpoint_buffer_allocator_->Alloc(hash_join_checkpoint_size);
            if(status) {
                alloc_buffer = buffer;
                break;
            } else {
                std::cout << "waiting for free buffer.\n";
                op_checkpoint_not_full_.wait(lock);
            }
        }while(true);

        actual_size = hash_join_state.serialize(alloc_buffer);
        // if(add_cktp_cnts == 13) {
        //     std::cout << "HashJoinOperatorState actual size: " << actual_size << std::endl;
        //     assert(0);
        // }
        if(actual_size != hash_join_checkpoint_size) {
            std::cout << "actual_size: " << actual_size << ", hash_join_checkpoint_size: " << hash_join_checkpoint_size << std::endl;
            assert(0);
        }
        // assert(actual_size == hash_join_checkpoint_size);
        write_status = true;

        op_checkpoint_queue_.push(OpCheckpointBlock{.buffer = alloc_buffer, .size = actual_size});
        op_checkpoint_not_empty_.notify_all();
    }
    else if(auto sort_op = dynamic_cast<SortExecutor *>(abstract_executor)) {
        SortOperatorState sort_join_state(sort_op);
        size_t sort_join_checkpoint_size = sort_join_state.getSize();

        // if(add_cktp_cnts == 13) std::cout << "SortOperatorState size: " << sort_join_checkpoint_size << std::endl;

        char* alloc_buffer;
        do {
            auto [status, buffer] = op_checkpoint_buffer_allocator_->Alloc(sort_join_checkpoint_size);
            if(status) {
                alloc_buffer = buffer;
                break;
            } else {
                std::cout << "waiting for free buffer.\n";
                op_checkpoint_not_full_.wait(lock);
            }
        }while(true);

        actual_size = sort_join_state.serialize(alloc_buffer);
        assert(actual_size == sort_join_checkpoint_size);
        write_status = true;

        op_checkpoint_queue_.push(OpCheckpointBlock{.buffer = alloc_buffer, .size = actual_size});
        op_checkpoint_not_empty_.notify_all();
    }
    else if(auto projection_op = dynamic_cast<ProjectionExecutor *>(abstract_executor)) {
        assert(projection_op->is_root_);
        ProjectionOperatorState projection_state(projection_op);
        size_t projection_checkpoint_size = projection_state.getSize();

        // if(add_cktp_cnts == 13) std::cout << "ProjectionOperatorState size: " << projection_checkpoint_size << std::endl;

        char* alloc_buffer;
        do {
            auto [status, buffer] = op_checkpoint_buffer_allocator_->Alloc(projection_checkpoint_size);
            if(status) {
                alloc_buffer = buffer;
                break;
            } else {
                std::cout << "waiting for free buffer.\n";
                op_checkpoint_not_full_.wait(lock);
            }
        }while(true);

        actual_size = projection_state.serialize(alloc_buffer);
        assert(actual_size == projection_checkpoint_size);
        write_status = true;

        op_checkpoint_queue_.push(OpCheckpointBlock{.buffer = alloc_buffer, .size = actual_size});
        op_checkpoint_not_empty_.notify_all();
    }
    else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;

        return {false, 0};
    }

    // add_cktp_cnts++;
    // std::cout << "add operator size: " << actual_size << "\n";
    write_tot_size += actual_size;
    write_cnts++;

    return {write_status, actual_size};
    
}


void OperatorStateManager::write_operator_state_to_state_node() {
    std::unique_lock<std::mutex> lock(op_latch_);

    while(op_checkpoint_queue_.empty()) {
        op_checkpoint_not_empty_.wait(lock);
    }

    OpCheckpointBlock op_checkpoint_block = op_checkpoint_queue_.front();
    op_checkpoint_queue_.pop();

    /*
        write to state node
    */
    size_t remote_offset = meta_manager_->GetJoinBlockAddrByThread(coro_sched_->t_id_);
    
    // switch(op_checkpoint_block.ckpt_type_) {
    //     case CkptType::HashJoinHashTableCkpt: {
    //         remote_offset += *reinterpret_cast<int *>(op_checkpoint_block.buffer);
    //         if(!coro_sched_->RDMAWriteSync(0, op_checkpoint_qp_, op_checkpoint_block.buffer + sizeof(int), remote_offset, op_checkpoint_block.size)) {
    //             RDMA_LOG(ERROR) << "Failed to write operator state into state_node.";
    //             assert(0);
    //         }
    //     } break;
    //     case CkptType::BlockJoinCkpt:
    //     case CkptType::IndexScanCkpt:
    //     case CkptType::HashJoinOpCkpt: {
    //         if(!coro_sched_->RDMAWriteSync(0, op_checkpoint_qp_, op_checkpoint_block.buffer, remote_offset + op_next_write_offset_, op_checkpoint_block.size)) {
    //             RDMA_LOG(ERROR) << "Failed to write operator state into state_node.";
    //             assert(0);
    //         }
    //     } break;
    // }
    
    if(!coro_sched_->RDMAWriteSync(0, op_checkpoint_qp_, op_checkpoint_block.buffer, remote_offset + op_next_write_offset_, op_checkpoint_block.size)) {
        std::cout << "write size: " << op_checkpoint_block.size /1024/1024 << "MB\n";
        RDMA_LOG(ERROR) << "Failed to write operator state into state_node.";
        assert(0);
    }

    /*
        update checkpoint meta
    */
    std::unique_lock<std::mutex> op_meta_lock(op_meta_latch_);

    // std::cout << "op_checkpoint_block.size: " << op_checkpoint_block.size << "\n";
    ck_meta_->checkpoint_num ++;
    ck_meta_->total_size += op_checkpoint_block.size;    
        
    write_op_checkpoint_meta();
    // std::cout << "op_checkpoint_block_size: " << op_checkpoint_block.size << "\n";

    /*
        update next_write_offset_
    */
    int prev_offset = op_next_write_offset_;
    op_next_write_offset_ += op_checkpoint_block.size;
    

    /*
        free buffer
    */
    op_checkpoint_buffer_allocator_->Free(op_checkpoint_block.size);

    // RwServerDebug::getInstance()->DEBUG_PRINT("[WRITE OP STATE INFO][T_ID: " + std::to_string(coro_sched_->t_id_) + " buffer: " + std::to_string(reinterpret_cast<uintptr_t>(op_checkpoint_block.buffer)) + ", remote offset: " + std::to_string(prev_offset) + ", size: " + std::to_string(op_checkpoint_block.size));
    /*
        notify
    */
    op_checkpoint_not_full_.notify_all();
}

void OperatorStateManager::write_op_state_thread() {
    while(1) {
        write_operator_state_to_state_node();
        // write_tot_size = ck_meta_->total_size;
        // std::cout << "write block " << cnt << "\n";
    }
}

void OperatorStateManager::clear_op_checkpoint() {
    std::unique_lock<std::mutex> op_lock(op_latch_);
    std::unique_lock<std::mutex> op_meta_lock(op_meta_latch_);

    /*
        clear op checkpoint
    */
    while(!op_checkpoint_queue_.empty()) {
        auto op_checkpoint_block = op_checkpoint_queue_.front();
        op_checkpoint_queue_.pop();
        op_checkpoint_buffer_allocator_->Free(op_checkpoint_block.size);
    }

    /*
        clear op meta
    */
    if(!(ck_meta_->checkpoint_num == 0) || !(ck_meta_->total_size == 0)) {
        ck_meta_->checkpoint_num = 0;
        ck_meta_->total_size = 0;
        write_op_checkpoint_meta();
    }

    op_next_write_offset_ = CheckPointMetaSize;
}

void OperatorStateManager::clear_op_meta() {
    ck_meta_->checkpoint_num = 0;
    ck_meta_->total_size = 0;
    op_next_write_offset_ = CheckPointMetaSize;
}

/*
    无线程保护，需要调用者做并发保护
*/
void OperatorStateManager::write_op_checkpoint_meta() {

    /*
        get remote offset
        写checkpoint meta不用加checkpoint meta size
    */
    size_t offset = meta_manager_->GetJoinBlockAddrByThread(coro_sched_->t_id_);
    
    /*
        将meta写入local rdma memory
    */
    ck_meta_->serialize(op_checkpoint_meta_buffer_);
    if(!coro_sched_->RDMAWriteSync(0, op_checkpoint_qp_, op_checkpoint_meta_buffer_, offset, CheckPointMetaSize)) {
        RDMA_LOG(ERROR) << "Failed to write operator state meta into state_node. offset=" << offset << ", checkpointmetasize=" << CheckPointMetaSize;
        assert(0);
    }

    /*
        debug log
    */
    // RwServerDebug::getInstance()->DEBUG_PRINT("[WRITE OP META][T_ID: " + std::to_string(coro_sched_->t_id_) + "][CHECKPOINT NUM: " + std::to_string(ck_meta_->checkpoint_num) + "][TOTAL SIZE: " + std::to_string(ck_meta_->total_size) + "][total src op: " + std::to_string(total_src_op) + "]: write op meta to state node.");
    
    return ;
}

std::unique_ptr<CheckPointMeta> OperatorStateManager::read_op_checkpoint_meta() {
    std::unique_lock<std::mutex> lock(op_meta_latch_);

    // get remote offset
    size_t offset = meta_manager_->GetJoinBlockAddrByThread(coro_sched_->t_id_);

    /*
        read op checkpoint meta from remote state node
    */
    if(!coro_sched_->RDMAReadSync(0, op_checkpoint_qp_, op_checkpoint_meta_buffer_, offset, CheckPointMetaSize)) {
        RDMA_LOG(ERROR) << "Failed to read operator state meta from state_node.";
        assert(0);
    }

    /*
        deserialize
    */
    auto ck_meta = std::make_unique<CheckPointMeta>();
    bool status = ck_meta->deserialize(op_checkpoint_meta_buffer_, CheckPointMetaSize);
    if(!status) {
        std::cerr << "Failed to deserialize checkpoint meta.\n";
    }

    return std::move(ck_meta);
}

std::vector<std::unique_ptr<OperatorState>> OperatorStateManager::read_op_checkpoints(CheckPointMeta *ck_meta) {
    std::unique_lock<std::mutex> lock(op_latch_);

    std::vector<std::unique_ptr<OperatorState>> op_states;

    /*
        check ck_meta
    */
    if(ck_meta->thread_id < 0) {
        return op_states;
    }

    /*
        检查thread id
    */
    if(ck_meta->thread_id != coro_sched_->t_id_) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        assert(false);
    }

    if(ck_meta->checkpoint_num <= 0) {
        return op_states;
    }


    /*
        read all op states from state node
    */
        // get remote_offset
    size_t remote_offset = meta_manager_->GetJoinBlockAddrByThread(ck_meta->thread_id);
        // alloc buffer
    auto buffer = op_checkpoint_read_cache_allocator_->Alloc(ck_meta->total_size);

    /*
        read all op states
    */
    if(!coro_sched_->RDMAReadSync(0, op_checkpoint_qp_, buffer, remote_offset + CheckPointMetaSize, ck_meta->total_size)) {
        std::cout << "try to read total_size: " << ck_meta->total_size << "\n";
        RDMA_LOG(ERROR) << "Failed to read operator states from state_node.";
        assert(0);
    }

    size_t offset = 0;
    /*
        解析
    */
    for(int i = 0; i < ck_meta->checkpoint_num; ++i) {
        // std::cout << "Deserialize checkpoint " << i << " state header\n";
        auto op_stat = std::make_unique<OperatorState>();
        op_stat->deserialize(buffer + offset, op_stat->getSize());
        op_stat->op_state_addr_ = buffer + offset;

        // std::cout << "op_state_addr: " << op_stat->op_state_addr_ << "\n";

        offset += op_stat->op_state_size_;
        // std::cout << "op_state_size: " << op_stat->op_state_size_ << "\n";

        // std::cout << "[sql_id_: " << op_stat->sql_id_ << "][op_id: " << op_stat->operator_id_ << "][op_state_size: " << op_stat->op_state_size_ << "][op_stat->op_state_time: " << op_stat->op_state_time_ << "][op_stat->exec_type: " << op_stat->exec_type_ << "]\n";

        op_states.push_back(std::move(op_stat));
    }

    return std::move(op_states);
}



