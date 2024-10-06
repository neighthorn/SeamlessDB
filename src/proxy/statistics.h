#pragma once

#include <mutex>
#include <thread>
#include <atomic>
#include <fstream>
#include <assert.h>

class Statistics {
public:
    static bool create_instance() {
        statistics_ = new Statistics();
        // std::thread()
        return true;
    }
    static void destroy_instance() {
        delete statistics_;
    }
    static Statistics* get_instance() {
        assert(statistics_ != nullptr);
        return statistics_;
    }

    void add_commit_txn_count() {
        // std::lock_guard<std::mutex> lock(mutex_);
        ++ commit_txn_count_;
    }

    void add_abort_txn_count() {
        // std::lock_guard<std::mutex> lock(mutex_);
        ++ abort_txn_count_;
    }

    void calc_throughtput_in_last_second() {
        std::ofstream outfile("../result/test_result.csv", std::ios::trunc);
        std::ofstream abortfile("../result/abort_result.csv", std::ios::trunc);
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            {
                // std::lock_guard<std::mutex> lock(mutex_);
                // int tmp = commit_txn_count_;
                throuput_in_last_second_ = commit_txn_count_ - commit_txn_in_last_second_;
                commit_txn_in_last_second_ = commit_txn_count_;
                abort_tp_in_last_second_ = abort_txn_count_ - abort_txn_in_last_second_;
                abort_txn_in_last_second_ = abort_txn_count_;
            }
            outfile << throuput_in_last_second_ << ",";
            outfile.flush();
            abortfile << abort_tp_in_last_second_ << ",";
            abortfile.flush();
        }
    }

    void start_time_throughput_calc() {
        time_throughput_thread_ = std::thread(&Statistics::calc_throughtput_in_last_second, this);
    }

    // std::mutex mutex_;
    // int commit_txn_count_;
    std::atomic<int> commit_txn_count_;
    // int abort_txn_count_;
    std::atomic<int> abort_txn_count_;
    int commit_txn_in_last_second_;
    int throuput_in_last_second_;
    int abort_txn_in_last_second_;
    int abort_tp_in_last_second_;
    std::thread time_throughput_thread_;

private:
    Statistics() :commit_txn_count_(0), abort_txn_count_(0), commit_txn_in_last_second_(0), abort_txn_in_last_second_(0) {}
    static Statistics* statistics_;
};