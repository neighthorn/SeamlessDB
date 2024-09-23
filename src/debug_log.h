#pragma once

#include <iostream>
#include <string>
#include <mutex>

#include "recovery/redo_log/redo_log.h"

#include "spdlog/sinks/basic_file_sink.h"

#define PRINT_LOG 1

class RwServerDebug {
private:
    inline static RwServerDebug* instance = nullptr;
    inline static std::mutex latch;

    std::string     log_file = "rw_server_" + std::to_string(state_theta_) + "_" + std::to_string(block_size_) + "_" + std::to_string(time(nullptr)) + ".log";
    std::shared_ptr<spdlog::logger> logger;



    RwServerDebug() {
        std::cout << "RwServerDebug instance created." << std::endl;
        logger = spdlog::basic_logger_mt("rw_server", log_file);
        logger->flush_on(spdlog::level::err);
    }

    RwServerDebug(const RwServerDebug&) = delete;
    RwServerDebug& operator=(const RwServerDebug&) = delete;

public:
    static RwServerDebug* getInstance() {
        std::scoped_lock<std::mutex> lock{latch};
        if (instance == nullptr) {
            instance = new RwServerDebug();
        }
        return instance;
    }

    void DEBUG_RECEIVE_SQL(int client_fd, char *sql) {
        #ifdef PRINT_LOG
            logger->info("[RECEIVE SQL][ClientFd: {}][SQL: {}]", client_fd, sql);
        #endif
    }

    void DEBUG_WRITE_PLAN(int plan_node, int plan_size, const std::string &sql) {
        #ifdef PRINT_LOG
            logger->info("[WRITE PLAN TO STATE][plan node: {}][plan size: {}][sql: {}]", plan_node, plan_size, sql);
        #endif
    }

    void DEBUG_WRITE_BLOCK(int block_size) {
        #ifdef PRINT_LOG
            logger->info("[WRITE JOIN BLOCK TO STATE][block size: {}]", block_size);
        #endif
    }

    void DEBUG_ABORT(const std::string &sql, const std::string &reason) {
        #ifdef PRINT_LOG
            logger->info("[ABORT][sql: {}][reason: {}]", sql, reason);
        #endif
    }

    void DEBUG_PRINT(const std::string &log) {
        #ifdef PRINT_LOG
            logger->info("[PRINT][{}]", log);
            logger->flush();
        #endif
    }

    static void releaseInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
            // std::cout << " RwServerDebug instance destroyed." << std::endl;
        }
    }
};

class StorageDebug {
private:
    inline static StorageDebug* instance = nullptr;
    const std::string storage_log = "storage.log";

    std::shared_ptr<spdlog::logger> logger;

    StorageDebug() {
        std::cout << " StorageDebug instance created." << std::endl;
        logger = spdlog::basic_logger_mt("storage_pool", storage_log);
        logger->flush_on(spdlog::level::err);
    }

    StorageDebug(const StorageDebug&) = delete;
    StorageDebug& operator=(const StorageDebug&) = delete;

public:
    static StorageDebug* getInstance() {
        if (instance == nullptr) {
            instance = new StorageDebug();
        }
        return instance;
    }

    static void releaseInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
            // std::cout << " StorageDebug instance destroyed." << std::endl;
        }
    }

    void DEBUG_REPALY_LOG(const RedoLogRecord &redo_log_hdr) {
        #ifdef PRINT_LOG
            // logger->info("[REPLAY LOG][LSN: {}][TXN: {}][LOG TYPE: {}]",
            //     redo_log_hdr.lsn_, redo_log_hdr.log_tid_, RedoLogTypeStr[redo_log_hdr.log_type_]);
        #endif 
    }

    void DEBUG_WRITE_LOG(const int lsn_t, bool status, const std::string &log) {
        #ifdef PRINT_LOG
            // logger->info("[WRITE LOG][LSN: {}][Status: {}][log: ]", lsn_t, status, log);
        #endif
    }

    void DEBUG_PRINT_LOG(const std::string &log) {
        #ifdef PRINT_LOG
            logger->info("[PRINT LOG][{}]", log);
        #endif
    }
};


/*
    Proxy Log Debug
*/
class ProxyLog {
private:
    inline static ProxyLog* instance = nullptr;
    inline static std::mutex latch_;

    const std::string proxy_log = "proxy_log.log";

    std::shared_ptr<spdlog::logger> logger;
    ProxyLog() {
        logger = spdlog::basic_logger_mt("proxy", proxy_log);
        logger->flush_on(spdlog::level::err);
        // std::cout << " ProxyLog instance created." << std::endl;
    }

    ProxyLog(const ProxyLog&) = delete;
    ProxyLog& operator=(const ProxyLog&) = delete;

public:
    static ProxyLog* getInstance() {
        std::scoped_lock<std::mutex> lock(latch_);
        if (instance == nullptr) {
            instance = new ProxyLog();
        }
        return instance;
    }

    /*
        debug send sql
    */
    void DEBUG_SEND_SQL(int client_fd, const std::string &sql) {
        #ifdef PRINT_LOG
            logger->info("[SEND SQL][ClientFd: {}][SQL: {}]", client_fd, sql);
        #endif
    }

    static void releaseInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
            // std::cout << " ProxyLog instance destroyed." << std::endl;
        }
    }
};
