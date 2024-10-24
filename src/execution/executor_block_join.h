/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "debug_log.h"
#include "errors.h"


/*
    前向声明
*/
class JoinBlock;
class JoinBlockExecutor;
class BlockNestedLoopJoinExecutor;

class BlockJoinOperatorState;

/* 
    Join Block 保存一系列的record  
*/
class JoinBlock {
    public:
        std::vector<std::unique_ptr<Record>> buffer_;
        const size_t MAX_SIZE;
        size_t size_; // 记录大小
        size_t cur_pos_;

        JoinBlock(size_t max_size);

        void push_back(std::unique_ptr<Record> record);

        void beginTuple();

        void nextTuple();

        Record* get_record();

        void reset();
    
        bool is_full();

        bool is_end() ;

        // 序列化JoinBlock到dest指针指向的内存区域
        int serialize_data(char* dest);

        /*
            record_length 不包括sizeof(hdr)
        */
        size_t deserialize(char *src, int record_num, int current_pos, int record_length) ;

        size_t getEstimateSize();
};
class JoinBlockExecutor {
public:
    BlockNestedLoopJoinExecutor         *father_exec_;          // 父亲算子
    AbstractExecutor                    *executor_ = nullptr;   // 儿子算子
    std::unique_ptr<JoinBlock>          join_block_ = nullptr;
    int                                 current_block_id_;      // block id

    JoinBlockExecutor(BlockNestedLoopJoinExecutor *father_exec, AbstractExecutor* exe);

    void beginBlock();
    /*
        恢复用
    */
    void load_block_info(BlockJoinOperatorState *block_op_state);

    void nextBlock() ;
    JoinBlock* Next() ;
    bool is_end() ;
};

/*
    记录建立检查点时的信息
*/
struct BlockCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;       // 记录建立检查点时的时间戳
    int left_block_id_;         // 记录建立检查点时的left block id
    size_t left_block_size_;    // 记录建立检查点时的left block 大小
    double left_rc_op_;         // 左子树重做到当前检查点的rc_op
    int state_change_time_;
};

class BlockNestedLoopJoinExecutor : public AbstractExecutor {
friend class BlockJoinOperatorState;
friend class JoinBlockExecutor;

public:
    std::shared_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::shared_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    int state_change_time_;

public:

    /*  
       join block executor
    */ 
    std::unique_ptr<JoinBlockExecutor> left_blocks_;

    JoinBlock   *left_block_ = nullptr;

    std::vector<BlockCheckpointInfo>    ck_infos_;  // 记录建立检查点时的信息

    // int     left_child_call_times_;     // 左儿子调用次数
    // int     be_call_times_;             // 被调用次数


   public:
    BlockNestedLoopJoinExecutor(std::shared_ptr<AbstractExecutor> left, std::shared_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds, Context *context, int sql_id, int operator_id) ;


    inline const std::vector<ColMeta> &cols() const override{
        return cols_;
    };

    inline bool is_end() const override { 
        return isend; 
    };

    inline size_t tupleLen() const override { return len_; };

    inline std::string getType() override { return "BlockNestedLoopJoinExecutor"; };

    // 
    void beginTuple() override  ;

    void nextTuple() override   ;

    std::unique_ptr<Record> Next() override ;


    // override but not implemented
    inline Rid &rid() override { return _abstract_rid; }
    inline ColMeta get_col_offset(const TabCol &target) override { return ColMeta();};
    inline int checkpoint(char* dest) override { return -1; }

    

public:

    // 找到下一个符合fed_cond的tuple
    void find_next_valid_tuple();

    /*
        根据代价估计函数，判断是否需要写状态
    */
    std::pair<bool, double> judge_state_reward(BlockCheckpointInfo *current_ck_info) ;

    /*
        递归计算RC op
    */
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock>  current_time) ;

    /*
        判断是否满足写op checkpoint的条件并写入
    */
    /*  
        type = 0: 默认情况
        type = 1: consume一个block就写状态，绕过代价估计
    */
    void write_state_if_allow(int type = 0);

public:
    /*
        load block join state
    */
    void load_state_info(BlockJoinOperatorState *block_join_op) ;

    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};