#pragma once

#include <stdexcept>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <iostream>

#include "benchmark/test/test_wk.h"
#include "benchmark/tpcc/tpcc_wk.h"
#include "benchmark/tpch/tpch_wk.h"

class ConnectionClosedException: public std::exception {
public:
    const char* what() const noexcept override {
        return "Connection to server has been closed.";
    }
};

class Proxy {
public:
    Proxy(std::string workload, int rw_thread_num, int record_num) {
        rw_node_thread_num_ = rw_thread_num;
        workload_ = workload;
        std::cout << "workload: " << workload_ << "\n";

        if(workload_.compare("test") == 0) {
            bench_mark_ = new TestWK(nullptr, nullptr, record_num, nullptr);
            bench_mark_->init_transaction(rw_node_thread_num_);
        }
        else if(workload_.compare("tpcc") == 0) {
            bench_mark_ = new TPCCWK(nullptr, nullptr, record_num, nullptr);
            bench_mark_->init_transaction(rw_node_thread_num_);
        } 
        else if(workload_.compare("tpch") == 0) {
            bench_mark_ = new TPCHWK(nullptr, nullptr, record_num, nullptr);
            bench_mark_->init_transaction(rw_node_thread_num_);
        } 
        else {
            std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        }
    }

    void run();

    std::vector<std::thread> rw_threads_;

    std::string rw_node_ip_;
    int rw_node_port_;
    int rw_node_thread_num_;

    std::string back_rw_ip_;
    int back_rw_port_;

    std::string workload_;
    BenchMark* bench_mark_;
};