#pragma once

#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

class StateServer {
public:
    StateServer(int nid, int local_port, size_t txn_list_size, size_t log_buf_size, size_t lock_buf_size,
                size_t sql_buf_size, size_t plan_buf_size, size_t join_plan_size, size_t join_block_size)
        : server_node_id(nid), local_port(local_port), txn_list_size(txn_list_size), 
        log_buf_size(log_buf_size), lock_buf_size(lock_buf_size),
        sql_buf_size(sql_buf_size), plan_buf_size(plan_buf_size),
        join_plan_size(join_plan_size), join_block_size(join_block_size)
        {}

    void AllocMem();

    void InitMem();

    void InitRDMA();

    // void SendMeta();

    bool Run();

    void CleanQP();

private:
    const int server_node_id;
    const int local_port;
    const size_t txn_list_size;
    const size_t log_buf_size;
    const size_t lock_buf_size;
    const size_t sql_buf_size;
    const size_t plan_buf_size;
    /*
        join state size
    */
    const size_t join_plan_size;
    const size_t join_block_size;

    RdmaCtrlPtr rdma_ctrl;
    char* txn_list;                 // the start address of active transactions list
    char* log_buffer;               // the start address of redo log buffer
    char* lock_buffer;              // the start address of lock buffer
    char* sql_buffer;               // the start address of sql buffer
    char* plan_buffer;              // the start address of plan buffer
    
    /*
        join state
    */
    char *join_plan_buffer;
    char *join_block_buffer;
};