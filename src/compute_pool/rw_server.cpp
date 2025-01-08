#include <cstdio>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <stack>
#include <chrono>

#include "errors.h"
#include "parser/ast.h"
#include "rw_server.h"
#include "common/macro.h"
#include "util/json_util.h"
#include "util/state_util.h"
#include "state/allocator/offset_allocator.h"
#include "storage/storage_service.pb.h"
#include "state/state_item/op_state.h"
#include "state/resume_util.h"
#include "execution/comp_ckpt_mgr.h"

#include "debug_log.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:12190", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

// #define TIME_OPEN 0

int state_open_ = 0;
double state_theta_ = -1.0;
double src_scale_factor_ = 1000.0;
int block_size_ = 500;
int node_type_ = 0; // rw_server default
int MB_ = 8192;
int RB_ = 8192;
int C_ = 1000;
int cost_model_ = 0;
int interval_ = 0;
bool write_ckpt_ = true;

int back_up_resumption_ = 0;

int parallel_factor = 1;

int* commit_txns;
int* abort_txns;
int client_num;
std::chrono::_V2::system_clock::time_point server_start_time;
std::chrono::_V2::system_clock::time_point server_end_time;

static jmp_buf jmpbuf;
bool should_exit = false;
void sigint_handler(int signo) {
    server_end_time = std::chrono::high_resolution_clock::now();
    int64_t tot_duration = std::chrono::duration_cast<std::chrono::milliseconds>(server_end_time - server_start_time).count();

    should_exit = true;
    // log_mgr_->flush_log_to_disk();
    // sm_manager->close_db();
    std::cout << "The Server receive Crtl+C, will been closed\n";

    int tot_commit_txn = 0;
    int tot_abort_txn = 0;

    for(int i = 0; i < client_num; ++i) {
        tot_commit_txn += commit_txns[i];
        tot_abort_txn += abort_txns[i];
    }

    double commit_tput = (double)tot_commit_txn / ((double)tot_commit_txn / 1000.0);
    double abort_tput = (double)tot_abort_txn / ((double)tot_commit_txn / 1000.0);

    std::cout << "commit_tput: " << commit_tput << ", abort_tput: " << abort_tput << "\n";
    RwServerDebug::getInstance()->DEBUG_PRINT("[commit_tput: " + std::to_string(commit_tput) + "][abort_tput: " + std::to_string(abort_tput) + "]");

    longjmp(jmpbuf, 1);
}

// // 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
// void SetTransaction(txn_id_t *txn_id, Context *context, RWNode* node, int connection_id) {
//     // context->txn_ = node->txn_mgr_->get_transaction(*txn_id);
//     if(context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
//         context->txn_->get_state() == TransactionState::ABORTED) {
//         if(context->txn_ != nullptr) node->txn_mgr_->clear_txn(context->txn_);
//         context->txn_ = node->txn_mgr_->begin(nullptr, context->log_mgr_);
//         *txn_id = context->txn_->get_transaction_id();
//         context->txn_->set_txn_mode(false);

//         if(state_open_ && context->rdma_allocated_) {
//             MetaManager* meta_mgr = MetaManager::get_instance();
//             RCQP* qp = context->qp_mgr_->GetRemoteTxnListQPWithNodeID(0);
//             assert(qp != nullptr);
//             Transaction* txn = context->txn_;
//             if(context->state_slot_index_ == -1) {
//                 RegionBitmap* txn_list_bitmap = meta_mgr->txn_list_bitmap_;
//                 context->state_slot_index_ = txn_list_bitmap->get_first_free_bit();
//                 // context->state_slot_index_ = GetFirstFreeBit(txn_list_bitmap->bitmap_, txn_list_bitmap->bitmap_size_);
//                 SetBitToUsed(txn_list_bitmap->bitmap_, context->state_slot_index_);
//                 char* txn_list_bitmap_buff = context->rdma_buffer_allocator_->Alloc(txn_list_bitmap->bitmap_size_);
//                 memcpy(txn_list_bitmap_buff, (char*)txn_list_bitmap->bitmap_, txn_list_bitmap->bitmap_size_);
//                 if(!context->coro_sched_->RDMAWriteSync(0, qp, (char*)txn_list_bitmap_buff, meta_mgr->GetTxnListBitmapAddr(), txn_list_bitmap->bitmap_size_)) {
//                     RDMA_LOG(ERROR) << "Failed to write txn_list_bitmap into state_node";
//                     assert(0);
//                 }
//             }
//             TxnItem* txn_item_buf = (TxnItem*)context->rdma_buffer_allocator_->Alloc(sizeof(TxnItem));
//             txn_item_buf->txn_state = txn->get_state();
//             txn_item_buf->id = txn->get_transaction_id();
//             txn_item_buf->thread_id = connection_id;
//             txn_item_buf->txn_mode = txn->get_txn_mode();
//             GetHashCodeForTxn(txn_item_buf);
//             if(!context->coro_sched_->RDMAWriteSync(0, qp, (char*)txn_item_buf, meta_mgr->GetTxnAddrByIndex(context->state_slot_index_), sizeof(TxnItem))) {
//                 RDMA_LOG(ERROR) << "Failed to write txn item into state_node";
//                 assert(0);
//             }

//             // @TEST
//             context->coro_sched_->RDMAReadSync(0, qp, (char*)txn_item_buf, meta_mgr->GetTxnAddrByIndex(context->state_slot_index_), sizeof(TxnItem));
//             std::cout << "txn's id: " << txn->get_transaction_id() << ", txn_item read from state node's id: " << txn_item_buf->id << "\n";
//         }
//     }
// }

RWNode::RWNode(int local_rpc_port, std::string workload, int record_num, int thread_num, int buffer_pool_size, std::string config_path) 
    : local_rpc_port_(local_rpc_port), workload_(workload), record_num_(record_num), thread_num_(thread_num),
    buffer_pool_size_(buffer_pool_size), config_path_(config_path){

    // init page channel and log channel
    page_channel_ = new brpc::Channel();
    log_channel_ = new brpc::Channel();
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if(page_channel_->Init(FLAGS_server.c_str(), &options) != 0) {
        std::cout << "Failed to initialize page_channel.\n";
        exit(1);
    }
    if(log_channel_->Init(FLAGS_server.c_str(), &options) != 0) {
        std::cout << "Failed to initialize log_channel.\n";
        exit(1);
    }

    MetaManager::create_instance(config_path_);
    std::cout << "finish create meta manager\n";
    RDMARegionAllocator::create_instance(MetaManager::get_instance(), thread_num_);
    std::cout << "finish create rdma region allocator\n";
    QPManager::create_instance(thread_num_);
    std::cout << "finish create qp manager\n";
    QPManager::BuildALLQPConnection(MetaManager::get_instance());
    ContextManager::create_instance(thread_num);
    std::cout << "finishi create state manager\n";
    CompCkptManager::create_instance();
    
    // /*
    //     RDMA asyn write thread pool
    // */
    // thread_pool_ = new ThreadPool(16);
    // std::cout << "finish create RDMA asyn write thread pool\n";

    // create database components
    disk_mgr_ = new DiskManager();
    slice_mgr_ = new SliceMetaManager();
    buffer_pool_mgr_ = new BufferPoolManager(NodeType::COMPUTE_NODE, buffer_pool_size_, page_channel_, disk_mgr_, slice_mgr_);
    index_mgr_ = new IxManager(buffer_pool_mgr_, disk_mgr_);
    mvcc_mgr_ = new MultiVersionManager(disk_mgr_, buffer_pool_mgr_);
    sm_mgr_ = new SmManager(disk_mgr_, buffer_pool_mgr_, index_mgr_, mvcc_mgr_);
    lock_mgr_ = new LockManager();
    txn_mgr_ = new TransactionManager(lock_mgr_, sm_mgr_, thread_num_);
    ql_mgr_ = new QlManager(sm_mgr_, txn_mgr_);
    log_mgr_ = new LogManager(log_channel_, ContextManager::get_instance()->log_rdma_buffer_);
    planner_ = new Planner(sm_mgr_);
    optimizer_ = new Optimizer(planner_);
    portal_ = new Portal(sm_mgr_);
    analyze_ = new Analyze(sm_mgr_);

    // load test workload
    if(workload_ == "test") {
        TestWK* test_wk = new TestWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
        test_wk->create_table();
        // test_wk->load_meta();
    }
    else if(workload == "tpcc") {
        TPCCWK* tpcc_wk = new TPCCWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
        tpcc_wk->create_table();
        // tpcc_wk->load_meta();
    } 
    else if(workload == "tpch") {
        TPCHWK* tpch_wk = new TPCHWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
        tpch_wk->create_table();
        // tpch_wk->load_meta();
    }
    else {
        std::cerr << "workload not supported!\n";
    }
}

int get_connection_id(char* data) {
    int conn_id = 0;
    for(int i = 0; i < BUFFER_LENGTH && data[i] != '\0'; ++i) {
        conn_id = conn_id * 10 + data[i] - '0';
    }
    return conn_id;
}

void replay_log_for_resumption(SmManager* sm_mgr) {
    ContextManager* state_mgr = ContextManager::get_instance();
    brpc::Channel* lsn_channel_ = new brpc::Channel();
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    state_mgr->fetch_log_states();

    if(lsn_channel_->Init(FLAGS_server.c_str(), &options) != 0) {
        std::cout << "Failed to initialize lsn_channel.\n";
        exit(1);
    }

    storage_service::StorageService_Stub stub(lsn_channel_);
    storage_service::GetPersistLsnRequest request;
    storage_service::GetPersistLsnResponse* response = new storage_service::GetPersistLsnResponse;
    brpc::Controller* cntl = new brpc::Controller;
    brpc::CallId cid = cntl->call_id();

    stub.GetPersistLsn(cntl, &request, response, NULL);
    int persist_lsn = response->persist_lsn();

    // std::cout << "persist_lsn: " << persist_lsn << "\n";

    int64_t head = state_mgr->curr_log_head_;
    int64_t tail = state_mgr->curr_log_tail_;
    // std::cout << "replay: head = " << head << ", tail = " << tail << "\n";
    RedoLogRecord* redo_log = nullptr;
    while(head != tail) {
        redo_log = state_mgr->log_rdma_buffer_->read_log(head, persist_lsn);
        if(redo_log == nullptr) continue;
        switch(redo_log->log_type_) {
            case RedoLogType::UPDATE: {
                UpdateRedoLogRecord* update_redo_log = static_cast<UpdateRedoLogRecord*>(redo_log);
                std::string table_name = std::string(update_redo_log->table_name_, update_redo_log->table_name_size_);
                auto index_handle = sm_mgr->primary_index_[table_name].get();
                if(index_handle == nullptr) {
                    throw RMDBError("table name " + std::string(update_redo_log->table_name_) + " not found!");
                }
                index_handle->update_record(update_redo_log->rid_, update_redo_log->new_value_.data, nullptr);
            } break;
            case RedoLogType::DELETE: {
                DeleteRedoLogRecord* delete_redo_log = static_cast<DeleteRedoLogRecord*>(redo_log);
                std::string table_name = std::string(delete_redo_log->table_name_, delete_redo_log->table_name_size_);
                auto index_handle = sm_mgr->primary_index_[table_name].get();

                index_handle->update_record(delete_redo_log->rid_, delete_redo_log->delete_value_.data, nullptr);
            } break;
            case RedoLogType::INSERT: {
                InsertRedoLogRecord* insert_redo_log = static_cast<InsertRedoLogRecord*>(redo_log);
                std::string table_name = std::string(insert_redo_log->table_name_, insert_redo_log->table_name_size_);
                auto index_handle = sm_mgr->primary_index_[table_name].get();

                index_handle->replay_insert_record(insert_redo_log->rid_, insert_redo_log->key_, insert_redo_log->insert_value_.data);
            } break;
            default:
            break;
        }
    }
}

void client_handler(int* sock_fd, RWNode* node) {
    /*
        sql_id
    */
    int sql_id = 0;

    int fd = *sock_fd;
    pthread_mutex_unlock(node->sockfd_mutex);

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    
    char *data_send = new char[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    // txn_id_t txn_id = INVALID_TXN_ID;
    t_id_t connection_id = INVALID_CONN_ID;

    std::string output = "establish client connection, sockfd: " + std::to_string(fd) + "\n";
    std::cout << output;

    // proxy会为每个compute node维护一个connection_pool，每个compute_node最多同时处理MAX_CONN_LIMIT个客户端链接，
    // 每个客户端连接由proxy分配一个当前compute_node内唯一的connectionid，一旦建立连接，proxy会把connectionid发送给compute node用来标识client
    // 当发生重连后，client拥有的connectionid不会发生改变，用作状态的识别
    // 创建连接后发送的第一条指令是thread_name，也就是proxy为client分配的connectionid，使用pthread_setname_np和pthread_getname_np来设置和获取线程名称
    memset(data_recv, 0, BUFFER_LENGTH);
    i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);
    connection_id = get_connection_id(data_recv);
    pthread_setname_np(pthread_self(), (const char*)&connection_id);
    // TEST
    memset(data_recv, 0, BUFFER_LENGTH);
    std::cout << pthread_getname_np(pthread_self(), data_recv, BUFFER_LENGTH);
    std::cout << "pthread_name: " << *(int*)data_recv << "\n";

    const char* send_ok = "Set connection id successfully.\n";
    memcpy(data_send, send_ok, strlen(send_ok));
    write(fd, data_send, strlen(send_ok));

    yyscan_t scanner;
    if(yylex_init(&scanner)) {
        std::cout << "failed to init scanner\n";
        exit(0);
    }

    /*
        Coroutine Scheduler, QP Manager, Meta Manager
    */
    CoroutineScheduler* coro_sched = new CoroutineScheduler(connection_id, CORO_NUM);
    auto local_rdma_region_range = RDMARegionAllocator::get_instance()->GetThreadLocalRegion(connection_id);
    RDMABufferAllocator* rdma_buffer_allocator = new RDMABufferAllocator(local_rdma_region_range.first, local_rdma_region_range.second);
    QPManager* qp_mgr = QPManager::get_instance();
    bool rdma_allocated = true;
    Transaction* txn = node->txn_mgr_->get_transaction(connection_id);
    MetaManager* meta_mgr = MetaManager::get_instance();

    /*
        Operator State Manager
    */
    OperatorStateManager *op_state_manager = new OperatorStateManager(connection_id, coro_sched, meta_mgr, qp_mgr);

    Context* context = new Context(node->lock_mgr_, node->log_mgr_, txn, coro_sched, op_state_manager, qp_mgr, data_send, &offset, rdma_allocated);
    context->rdma_buffer_allocator_ = rdma_buffer_allocator;
    context->parallel_worker_num_ = parallel_factor;

    while (true) {
        // std::cout << "Waiting for request..." << std::endl;
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        // std::cout << "data_recv: " << data_recv << "\n";

        if (i_recvBytes == 0) {
            std::cout << "Maybe the client has closed" << std::endl;
            break;
        }
        if (i_recvBytes == -1) {
            std::cout << "Client read error!" << std::endl;
            break;
        }

        if (strcmp(data_recv, "exit") == 0) {
            std::cout << "Client exit." << std::endl;
            break;
        }
        else if (strcmp(data_recv, "crash") == 0) {
            std::cout << "Server crash" << std::endl;
            exit(1);
        }
        else if(strcmp(data_recv, "reconnect_prepare") == 0) {
            if(state_open_ == 1 && node_type_ == 0) {
                // @STATE: prepare for reconnection, the current thread is responsible for the lock recover and log replay

                // auto recover_start = std::chrono::high_resolution_clock::now();
                node->txn_mgr_->recover_active_txn_lists(context);
                // auto recover_end = std::chrono::high_resolution_clock::now();
                // auto recover_duration = std::chrono::duration_cast<std::chrono::microseconds>(recover_end - recover_start).count();
                // std::cout << "recover_txn_list_time: " << recover_duration << "\n";
                // std::cout << "finish recover active_txn_list\n";
                node->lock_mgr_->recover_lock_table(node->txn_mgr_->active_transactions_, client_num);
                // recover_end = std::chrono::high_resolution_clock::now();
                // recover_duration = std::chrono::duration_cast<std::chrono::microseconds>(recover_end - recover_start).count();
                // std::cout << "recover_lock_table_time: " << recover_duration << "\n";
                // std::cout << "finish recover lock_table\n";
                replay_log_for_resumption(node->sm_mgr_);
                // recover_end = std::chrono::high_resolution_clock::now();
                // recover_duration = std::chrono::duration_cast<std::chrono::microseconds>(recover_end - recover_start).count();
                // std::cout << "recover_log_time: " << recover_duration << "\n";
                // std::cout << "finish recover log\n";
            } 
            else if(state_open_ == 1 && node_type_ == 1){    
                #ifdef TIME_OPEN
                    auto reconnect_start = std::chrono::high_resolution_clock::now();
                #endif

                // @STATE: prepare for reconnection, the current thread is responsible for the lock recover and log replay
                
                // node->lock_mgr_->recover_lock_table();
                // TODO: log replay
                /*
                    手动创建事务
                */
                node->txn_mgr_->begin(context->txn_, context->log_mgr_);

                /*
                    read state from remote
                */
                #ifdef TIME_OPEN
                    auto read_state_start = std::chrono::high_resolution_clock::now();
                #endif
                auto sql_state = context->op_state_mgr_->read_sql_from_state();
                /*
                    read op meta and op checkpoints
                */
                auto op_ck_meta = context->op_state_mgr_->read_op_checkpoint_meta();
                std::cout << "Read Op CK meta: checkpoint_num: " << op_ck_meta->checkpoint_num << ", total_size: " << op_ck_meta->total_size << "op_ck_meta->total_src_op: " << op_ck_meta->total_src_op << "\n";
                RwServerDebug::getInstance()->DEBUG_PRINT("[READ OP META FROM STATE][Before][thread id: " + std::to_string(op_ck_meta->thread_id) + "][checkpoint_num: " + std::to_string(op_ck_meta->checkpoint_num) + "][total size: " + std::to_string(op_ck_meta->total_size) + "][total srp: " + std::to_string(op_ck_meta->total_src_op));

                #ifdef TIME_OPEN
                    auto read_state_end = std::chrono::high_resolution_clock::now();
                    auto read_state_period = std::chrono::duration_cast<std::chrono::microseconds>(read_state_end - read_state_start).count();
                    std::cout << "time for read state: " << read_state_period << "\n";
                    RwServerDebug::getInstance()->DEBUG_PRINT("[time for read state:  " + std::to_string(read_state_period) + "]");
                #endif 
            
                if(op_ck_meta->checkpoint_num != 0) {
                    auto op_checkpoints = context->op_state_mgr_->read_op_checkpoints(op_ck_meta.get());
                    /*
                        重建exec plan
                    */
                    #ifdef TIME_OPEN
                        auto reconstruct_exec_plan_start = std::chrono::high_resolution_clock::now();
                    #endif

                    auto exec_plan = rebuild_exec_plan_from_state(node, context, sql_state.get(), op_ck_meta.get(), op_checkpoints);
                    #ifdef TIME_OPEN
                        auto reconstruct_exec_plan_end = std::chrono::high_resolution_clock::now();
                        auto reconstruct_exec_plan_period = std::chrono::duration_cast<std::chrono::microseconds>(reconstruct_exec_plan_end - reconstruct_exec_plan_start).count();
                        std::cout << "time for reconstruct exec plan: " << reconstruct_exec_plan_period << "\n";
                        RwServerDebug::getInstance()->DEBUG_PRINT("[time for reconstruct exec plan: " + std::to_string(reconstruct_exec_plan_period) + "]");
                    #endif

                    /*
                        断点续作
                    */
                    #ifdef TIME_OPEN
                        auto re_run_start = std::chrono::high_resolution_clock::now();
                    #endif
                    node->portal_->re_run(exec_plan, node->ql_mgr_, context);
                    
                    #ifdef TIME_OPEN
                        auto re_run_end = std::chrono::high_resolution_clock::now();
                        auto re_run_period = std::chrono::duration_cast<std::chrono::microseconds>(re_run_end - re_run_start).count();
                        std::cout << "time for re_run: " << re_run_period << "\n";
                        RwServerDebug::getInstance()->DEBUG_PRINT("[time for re_run: " + std::to_string(re_run_period) + "]");
                    #endif

                    // print_char_array(data_send, *context->offset_);
                }
                else {
                    /*
                        重建exec plan
                    */
                    #ifdef TIME_OPEN
                        auto reconstruct_exec_plan_start = std::chrono::high_resolution_clock::now();
                    #endif
                    auto exec_plan = rebuild_exec_plan_without_state(node, context, sql_state.get());

                    #ifdef TIME_OPEN
                        auto reconstruct_exec_plan_end = std::chrono::high_resolution_clock::now();
                        auto reconstruct_exec_plan_period = std::chrono::duration_cast<std::chrono::microseconds>(reconstruct_exec_plan_end - reconstruct_exec_plan_start).count();
                        std::cout << "time for reconstruct exec plan: " << reconstruct_exec_plan_period << "\n";
                        RwServerDebug::getInstance()->DEBUG_PRINT("[time for reconstruct exec plan: " + std::to_string(reconstruct_exec_plan_period) + "]");
                    #endif

                    /*
                        断点续作
                    */
                    #ifdef TIME_OPEN
                        auto re_run_start = std::chrono::high_resolution_clock::now();
                    #endif

                    node->portal_->run(exec_plan, node->ql_mgr_, context);
                    
                    #ifdef TIME_OPEN
                        auto re_run_end = std::chrono::high_resolution_clock::now();
                        auto re_run_period = std::chrono::duration_cast<std::chrono::microseconds>(re_run_end - re_run_start).count();
                        std::cout << "time for re_run: " << re_run_period << "\n";
                        RwServerDebug::getInstance()->DEBUG_PRINT("[time for re run: " + std::to_string(re_run_period) + "]");
                    #endif
                }
                #ifdef TIME_OPEN
                    auto reconnect_end = std::chrono::high_resolution_clock::now();
                    auto reconnect_period = std::chrono::duration_cast<std::chrono::microseconds>(reconnect_end - reconnect_start).count();
                    std::cout << "time for reconnect: " << reconnect_period << "\n";
                    RwServerDebug::getInstance()->DEBUG_PRINT("[time for reconnect: " + std::to_string(reconnect_period) + "]");
                #endif
            }


            std::string str = "success\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();
            if (write(fd, data_send, offset + 1) == -1) {
                std::cout << "fail\n"; 
                break;
            }
            // std::cout << "success\n";

            continue;
        }
        // else if(strcmp(data_recv, "reconnect") == 0) {
        //     // @STATE: reconnection, recover the transaction state and continue to execute

        // }

            // #ifdef TIME_OPEN
            //     auto reconnect_end = std::chrono::high_resolution_clock::now();
            //     auto reconnect_period = std::chrono::duration_cast<std::chrono::microseconds>(reconnect_end - reconnect_start).count();
            //     std::cout << "time for reconnect: " << reconnect_period << "\n";
            // #endif
            // continue;
        // }
        else if(strcmp(data_recv, "reconnect") == 0) {
            // @STATE: reconnection, recover the transaction state and continue to execute

        }

        if(state_open_) {
            context->op_state_mgr_->reset_remote_sql_and_op_region();
        }
        // @STATE: write sql into state_node
        if(state_open_) {
            context->op_state_mgr_->write_sql_to_state(sql_id, data_recv, i_recvBytes - 1);
        }

        #ifdef PRINT_LOG
            // std::cout << "Read from client " << fd << ": " << data_recv << std::endl;
            RwServerDebug::getInstance()->DEBUG_RECEIVE_SQL(fd, data_recv);
        #endif
        
        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        context->clear();
        // SetTransaction(&txn_id, context, node, connection_id);

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;

        // pthread_mutex_lock(node->buffer_mutex);
        YY_BUFFER_STATE buf = yy_scan_string(data_recv, scanner);
        if (yyparse(scanner) == 0) {
            if (ast::parse_tree != nullptr) {
                try {
                    #ifdef TIME_OPEN 
                        auto run_sql_start = std::chrono::high_resolution_clock::now();
                    #endif

                    // #ifdef TIME_OPEN
                    // auto analyze_start = std::chrono::high_resolution_clock::now();
                    // #endif
                    // analyze and rewrite
                    std::shared_ptr<Query> query = node->analyze_->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf, scanner);
                    finish_analyze = true;
                    // pthread_mutex_unlock(node->buffer_mutex);
                    // #ifdef TIME_OPEN
                    // auto analyze_end = std::chrono::high_resolution_clock::now();
                    // auto analyze_duration = std::chrono::duration_cast<std::chrono::microseconds>(analyze_end - analyze_start).count();
                    // std::cout << "time for analyze and rewrite: " << analyze_duration << "\n";
                    // RwServerDebug::getInstance()->DEBUG_PRINT("[time for analyze and rewrite: " + std::to_string(analyze_duration) + "]");
                    // #endif

                    // 优化器
                    // #ifdef TIME_OPEN
                    // auto optimize_start = std::chrono::high_resolution_clock::now();
                    // #endif
                    /*
                        设置planner的sql_id，并启动plan_query
                    */
                    node->optimizer_->set_planner_sql_id(sql_id);
                    std::shared_ptr<Plan> plan = node->optimizer_->plan_query(query, context);
                    // #ifdef TIME_OPEN
                    // auto optimize_end = std::chrono::high_resolution_clock::now();
                    // auto optimize_duration = std::chrono::duration_cast<std::chrono::microseconds>(optimize_end - optimize_start).count();
                    // std::cout << "time for optimization: " << optimize_duration << "\n";
                    // RwServerDebug::getInstance()->DEBUG_PRINT("[time for optimization: " + std::to_string(optimize_duration) + "]");
                    // #endif
                    // @STATE: write plan into state_node
                    if(state_open_) {
                        context->op_state_mgr_->write_plan_to_state(sql_id, node->sm_mgr_, plan);
                    }

                    // portal
                    // #ifdef TIME_OPEN
                    // auto start_start = std::chrono::high_resolution_clock::now();
                    // #endif
                    std::shared_ptr<PortalStmt> portalStmt = node->portal_->start(plan, context);
                    if(portalStmt->root != nullptr)
                        CompCkptManager::get_instance()->add_new_query_tree(portalStmt->root);
                    // #ifdef TIME_OPEN
                    // auto start_end = std::chrono::high_resolution_clock::now();
                    // auto start_duration = std::chrono::duration_cast<std::chrono::microseconds>(start_end - start_start).count();
                    // std::cout << "time for portal_start: " << start_duration << "\n";
                    // RwServerDebug::getInstance()->DEBUG_PRINT("[time for portal_start: " + std::to_string(start_duration) + "]");
                    // #endif

                    // #ifdef TIME_OPEN
                    // auto run_start = std::chrono::high_resolution_clock::now();
                    // #endif
                    // std::cout << "before run sql: " << data_recv << "\n";
                    node->portal_->run(portalStmt, node->ql_mgr_, context);
                    // std::cout << "after run sql: " << data_recv << "\n";
                    //  #ifdef TIME_OPEN
                    // auto run_end = std::chrono::high_resolution_clock::now();
                    // auto run_duration = std::chrono::duration_cast<std::chrono::microseconds>(run_end - run_start).count();
                    // std::cout << "time for portal_run: " << run_duration << "\n";
                    // RwServerDebug::getInstance()->DEBUG_PRINT("[time for portal_run: " + std::to_string(run_duration) + "]");
                    // #endif


                    #ifdef TIME_OPEN 
                        auto run_sql_end = std::chrono::high_resolution_clock::now();
                        auto run_sql_total = std::chrono::duration_cast<std::chrono::microseconds>(run_sql_end - run_sql_start).count();
                        std::cout << "time for run sql: " << run_sql_total << "\n";
                        RwServerDebug::getInstance()->DEBUG_PRINT("[time for run sql: " + std::to_string(run_sql_total) + "]");
                    #endif
                    
                    node->portal_->drop();
                } catch (TransactionAbortException &e) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    #ifdef PRINT_LOG
                        RwServerDebug::getInstance()->DEBUG_ABORT(std::string(data_recv, i_recvBytes), e.GetInfo());
                    #endif

                    // 回滚事务
                    node->txn_mgr_->abort(context->txn_, context);
                    // std::cout << e.GetInfo() << std::endl;
                    abort_txns[connection_id] ++;

                    // assert(0);
                } catch (RMDBError &e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    std::cerr << e.what() << std::endl;

                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;
                }
            }
        }

        sql_id++;
        /*
            test for operator checkpoint
        */
        {
            if(state_open_) {
                ContextManager::get_instance()->checkpoint();
                // auto op_ck_meta = context->op_state_mgr_->read_op_checkpoint_meta();
                // RwServerDebug::getInstance()->DEBUG_PRINT("[READ OP META FROM STATE][Before][thread id: " + std::to_string(op_ck_meta->thread_id) + "][checkpoint_num: " + std::to_string(op_ck_meta->checkpoint_num) + "][total size: " + std::to_string(op_ck_meta->total_size) + "]");

                context->op_state_mgr_->clear_op_checkpoint();

                // op_ck_meta = context->op_state_mgr_->read_op_checkpoint_meta();
                // RwServerDebug::getInstance()->DEBUG_PRINT("[READ OP META FROM STATE][After][thread id: " + std::to_string(op_ck_meta->thread_id) + "][checkpoint_num: " + std::to_string(op_ck_meta->checkpoint_num) + "][total size: " + std::to_string(op_ck_meta->total_size) + "]");
            }

        }
        
        if(finish_analyze == false) {
            yy_delete_buffer(buf, scanner);
            // pthread_mutex_unlock(node->buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if(strcmp(data_recv, "commit;") == 0) commit_txns[connection_id] ++;

        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
        // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        // if(context->txn_->get_txn_mode() == false)
        // {
        //     node->txn_mgr_->commit(context->txn_, context);
        //     commit_txns[connection_id] ++;
        // }
    }

    // Clear
    std::cout << "Terminating current client_connection..." << std::endl;
    close(fd);           // close a file descriptor.
    yylex_destroy(scanner);
    pthread_exit(NULL);  // terminate calling thread!
}

void RWNode::start_server() {
    // init buffer_mutex and sockfd_mutex
    buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(local_rpc_port_);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, thread_num_);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    std::vector<std::thread> threads;
    
    server_start_time = std::chrono::high_resolution_clock::now();

    for(int i = 0; i < client_num; ++i) {
        std::cout << "Waiting for new connection..." << std::endl;
        pthread_t thread_id;
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            std::cout << "Break from Server Listen Loop\n";
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            std::cout << "Accept error!" << std::endl;
            continue;  // ignore current socket ,continue while loop.
        }
        
        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        // if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd), this) != 0) {
        //     std::cout << "Create thread fail!" << std::endl;
        //     break;  // break while loop
        // }
        threads.push_back(std::thread(client_handler, &sockfd, this));
    }

    for(auto& t: threads) {
        t.join();
    }

    // Clear
    // std::cout << " Try to close all client-connection.\n";
    int ret = shutdown(sockfd_server, SHUT_WR);  // shut down the all or part of a full-duplex connection.
    if(ret == -1) { printf("%s\n", strerror(errno)); }
    //    assert(ret != -1);
    // sm_mgr_->close_db();
    // std::cout << " DB has been closed.\n";
    // std::cout << "Server shuts down." << std::endl;

    server_end_time = std::chrono::high_resolution_clock::now();
    int64_t tot_duration = std::chrono::duration_cast<std::chrono::milliseconds>(server_end_time - server_start_time).count();

    int tot_commit_txn = 0;
    int tot_abort_txn = 0;

    for(int i = 0; i < client_num; ++i) {
        tot_commit_txn += commit_txns[i];
        tot_abort_txn += abort_txns[i];
    }

    double commit_tput = (double)tot_commit_txn / ((double)tot_duration);
    double abort_tput = (double)tot_abort_txn / ((double)tot_duration);

    std::cout << "commit_tot_cnt: " << tot_commit_txn << ", time: " << tot_duration << "\n";
    std::cout << "commit_tput: " << commit_tput * 1000.0 << ", abort_tput: " << abort_tput << "\n";
    std::cout << "write op checkpoints: " << OperatorStateManager::write_cnts << "\n";
    std::cout << "write op checkpoint size(Bytes): " << OperatorStateManager::write_tot_size << "\n";
    RwServerDebug::getInstance()->DEBUG_PRINT("[commit_tot_cnt: " + std::to_string(tot_commit_txn) + "][time: " + std::to_string(tot_duration) + "]");
    RwServerDebug::getInstance()->DEBUG_PRINT("[commit_tput: " + std::to_string(commit_tput * 1000.0) + "][abort_tput: " + std::to_string(abort_tput) + "]");
    RwServerDebug::getInstance()->DEBUG_PRINT("[write op checkpoints: " + std::to_string(OperatorStateManager::write_cnts) + "]");
}

int main(int argc, char** argv) {
    if(argc < 3) {
        std::cout << "Please specify the server type: active/backup, rw/ro.";
        exit(1);
    }

    std::string config_path;

    if(strcmp(argv[1], "active") == 0) {
        config_path = "../src/config/compute_server_config.json";
    }
    else {
        config_path = "../src/config/compute_back_config.json";
        back_up_resumption_ = 1;
    }

    std::cout << config_path << "\n";

    if(strcmp(argv[2], "rw") == 0) {
        node_type_ = 0;
    }
    else {
        node_type_ = 1;
    }

    cJSON* cjson = parse_json_file(config_path);

    cJSON* node;
    if(node_type_ == 0) {
        node = cJSON_GetObjectItem(cjson, "rw_node");
    }
    else {
        node = cJSON_GetObjectItem(cjson, "ro_node");
    }

    int node_id = cJSON_GetObjectItem(node, "machine_id")->valueint;
    int local_rpc_port = cJSON_GetObjectItem(node, "local_rpc_port")->valueint;
    std::string workload = cJSON_GetObjectItem(node, "workload")->valuestring;
    int record_num = cJSON_GetObjectItem(node, "record_num")->valueint;
    int thread_num = cJSON_GetObjectItem(node, "thread_num")->valueint;
    state_open_ = cJSON_GetObjectItem(node, "state_open")->valueint;
    block_size_ = cJSON_GetObjectItem(node, "block_size")->valueint;
    state_theta_ = cJSON_GetObjectItem(node, "state_theta")->valuedouble;
    src_scale_factor_ = cJSON_GetObjectItem(node, "src_scale_factor")->valuedouble;
    int buffer_pool_size = cJSON_GetObjectItem(node, "buffer_pool_size")->valueint;
    MB_ = cJSON_GetObjectItem(node, "MB")->valueint;
    RB_ = cJSON_GetObjectItem(node, "RB")->valueint;
    C_ = cJSON_GetObjectItem(node, "C")->valueint;
    interval_ = cJSON_GetObjectItem(node, "interval")->valueint;
    int write_ckpt_num = cJSON_GetObjectItem(node, "write_ckpt")->valueint;
    if(write_ckpt_num == 1) write_ckpt_ = true;
    else write_ckpt_ = false;
    std::string cost_model_str = cJSON_GetObjectItem(node, "cost_model")->valuestring;
    if(cost_model_str.compare("SeamlessDB") == 0) {
        std::cout << "seamlessdb\n";
        cost_model_ = 0;
    }
    else if(cost_model_str.compare("PREDATOR") == 0) {
        std::cout << "predator\n";
        cost_model_ = 1;
    }
    else if(cost_model_str.compare("IntervalCkpt") == 0) {
        std::cout << "interval ckpt\n";
        cost_model_ = 2;
    }
    parallel_factor = cJSON_GetObjectItem(node, "parallel_factor")->valueint;

    std::cout << "cost_model: " << cost_model_ << ", interval: " << interval_ << "\n";
    
    client_num = thread_num;
    commit_txns = new int[client_num];
    abort_txns = new int[client_num];
    memset(commit_txns, 0, sizeof(int) * client_num);
    memset(abort_txns, 0, sizeof(int) * client_num);

    cJSON* state_node = cJSON_GetObjectItem(cjson, "remote_state_nodes");
    int sql_buf_size = cJSON_GetObjectItem(state_node, "sql_buf_size_GB")->valueint * 1024 * 1024 * 1024;
    int plan_buf_size = cJSON_GetObjectItem(state_node, "plan_buf_size_GB")->valueint * 1024 * 1024 * 1024;
    int cursor_buf_size = cJSON_GetObjectItem(state_node, "cursor_buf_size_GB")->valueint * 1024 * 1024 * 1024;

    cJSON_Delete(cjson);

    std::cout << "node_id: " << node_id << ", rpc_port: " << local_rpc_port << ", workload: " << workload << ", record_num: " << record_num << "\n";

    auto server = new RWNode(local_rpc_port, workload, record_num, thread_num, buffer_pool_size, config_path);
    server->thread_local_sql_size_ = sql_buf_size / thread_num;
    server->thread_local_plan_size_ = plan_buf_size / thread_num;
    server->thread_local_cursor_size_ = cursor_buf_size / thread_num;

    signal(SIGINT, sigint_handler);

    server->start_server();
    
    return 0;
}