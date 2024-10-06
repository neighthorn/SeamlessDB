#include "proxy.h"

#include "common/macro.h"
#include "util/json_util.h"
#include "statistics.h"

#include <chrono>

#define MAX_MEM_BUFFER_SIZE 8192
#define TXN_NUM_PER_THREAD 20
int client_num;

Statistics* Statistics::statistics_ = nullptr;

int* commit_txns;
int* abort_txns;
std::chrono::_V2::system_clock::time_point server_start_time;
std::chrono::_V2::system_clock::time_point server_end_time;
int* next_sql_index;

int init_tcp_sock(const char *server_host, int server_port) {
    struct hostent *host;
    struct sockaddr_in serv_addr;

    if ((host = gethostbyname(server_host)) == NULL) {
        fprintf(stderr, "gethostbyname failed. errmsg=%d:%s\n", errno, strerror(errno));
        return -1;
    }

    int sockfd;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        fprintf(stderr, "create socket error. errmsg=%d:%s\n", errno, strerror(errno));
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);
    serv_addr.sin_addr = *((struct in_addr *)host->h_addr);
    bzero(&(serv_addr.sin_zero), 8);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(struct sockaddr)) == -1) {
        fprintf(stderr, "Failed to connect. errmsg=%d:%s\n", errno, strerror(errno));
        close(sockfd);
        return -1;
    }
    return sockfd;
}

void send_recv_sql(int sockfd, std::string sql, char* recv_buf) {
    int send_bytes;
    // std::string str = "";
    // str += "sockfd: " + std::to_string(sockfd);

    // std::cout << sql << std::endl;

// mutex.lock();
    if((send_bytes = write(sockfd, sql.c_str(), sql.length() + 1)) == -1) {
        std::cerr << "send error: " << errno << ":" << strerror(errno) << " \n" << std::endl;
        exit(1);
    }
// mutex.unlock();
    // str += " finish send \n";
    // std::cout << str;

    int len = recv(sockfd, recv_buf, MAX_MEM_BUFFER_SIZE, 0);
    if (len < 0) {
        fprintf(stderr, "Connection was broken: %s\n", strerror(errno));
        throw ConnectionClosedException();
        return;
    } else if (len == 0) {
        printf("Connection has been closed\n");
        throw ConnectionClosedException();
        return;
    }
    // std::cout << sql << std::endl;
    // print_char_array(recv_buf, len);

    // str = "sockfd: " + std::to_string(sockfd)  + "       finish receive\n";
    // std::cout << str;

    // printf("%s\n", recv_buf);
}

void run_client(BenchMark* benchmark, std::string remote_ip, int remote_port, int thread_index) {
    int sockfd, send_bytes;
    char recv_buf[MAX_MEM_BUFFER_SIZE];
    sockfd = init_tcp_sock(remote_ip.c_str(), remote_port);

    // set connection id
    // std::cout << "thread " << thread_index << " begin to set connection_id\n";
    memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
    send_recv_sql(sockfd, std::to_string(thread_index), recv_buf);

    NativeTransaction* txn = nullptr;
    // std::vector<double> latencies;

    // Statistics* statistics = Statistics::get_instance();
    // int retry = 0;
    // std::ofstream longtxnfile("../result/longsql/configthread_" +  std::to_string(client_num) + "_" + std::to_string(thread_index) + "_sql.txt", std::ios::trunc);

    // int cnt = TXN_NUM_PER_THREAD;
    while(true) {
        // std::cout << "client " << thread_index << " begin generate_txn " << TXN_NUM_PER_THREAD - cnt << "\n";
        txn = benchmark->generate_transaction(thread_index);
        // std::cout << "client " << thread_index << " finished generate_txn " << TXN_NUM_PER_THREAD - cnt << "\n";
        // cnt --;
        // std::cout << "thread " << thread_index << ", cnt " << cnt << "\n";

        // auto start_time = std::chrono::high_resolution_clock::now();
        bool aborted = false;

        for(int i = 0; i < txn->queries.size(); ++i) {
            memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
            send_recv_sql(sockfd, txn->queries[i], recv_buf);
            if(strcmp(recv_buf, "abort\n") == 0) {
                // puts("abort");
                // statistics->add_abort_txn_count();
                // retry = 1;
                abort_txns[thread_index] ++;
                break;
            }
            // if(i == txn->queries.size() - 1) statistics->add_commit_txn_count();
            if(i == txn->queries.size() - 1) commit_txns[thread_index] ++;
        }

        // auto end_time = std::chrono::high_resolution_clock::now();
        // double latency = aborted ? 0 : std::chrono::duration<double, std::milli>(end_time - start_time).count();
        // latencies.push_back(latency);
        // if(latency > 1000) {
        //     for(int i = 0; i < txn->queries.size(); ++i)
        //         longtxnfile << txn->queries[i] << "\n";
        // }
    }

    close(sockfd);
    // longtxnfile.close();

    // std::ofstream outfile("../result/latency/configthread_" +  std::to_string(client_num) + "_" + std::to_string(thread_index) + "_latency.txt", std::ios::trunc);
    // for (const auto& latency : latencies) {
    //     outfile << latency << "\n";
    // }
    // outfile.close();
}

bool reconnect_to_backup_rw(std::string back_ip, int back_port, int thread_index) {
    std::cout << "thread " << thread_index << " begin to reconnect\n";
    int sock_fd, send_bytes;
    char recv_buf[MAX_MEM_BUFFER_SIZE];
    sock_fd = init_tcp_sock(back_ip.c_str(), back_port);

    memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
    send_recv_sql(sock_fd, std::to_string(thread_index), recv_buf);

    memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
    send_recv_sql(sock_fd, "reconnect", recv_buf);


}

bool notify_backup_rw_to_recover(std::string back_ip, int back_port) {
    int sock_fd, send_bytes;
    char recv_buf[MAX_MEM_BUFFER_SIZE];
    sock_fd = init_tcp_sock(back_ip.c_str(), back_port);

    memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
    send_recv_sql(sock_fd, 0, recv_buf);

    memset(recv_buf, '\0', MAX_MEM_BUFFER_SIZE);
    send_recv_sql(sock_fd, "reconnect_prepare", recv_buf);
    if(strcmp(recv_buf, "success\n") == 0) {
        close(sock_fd);
        return true;
    }

    close(sock_fd);
    return false;
}

void run_proxy(Proxy* proxy) {
    std::cout << "begin run proxy\n";
    std::mutex rw_mutex_;
    std::condition_variable rw_cv_;
    // bool all_threads_finished = false;
    int exception_thread_count = 0;
    bool recovered = false;
    // std::vector<std::exception_ptr> exceptions(proxy->rw_node_thread_num_);
    // Statistics::create_instance();
    // Statistics::get_instance()->start_time_throughput_calc();
    assert(proxy->bench_mark_ != nullptr);
    std::cout << "after create statistic instance\n";

    commit_txns = new int[proxy->rw_node_thread_num_];
    abort_txns = new int[proxy->rw_node_thread_num_];
    next_sql_index = new int[proxy->rw_node_thread_num_];
    memset(commit_txns, 0, sizeof(int) * proxy->rw_node_thread_num_);
    memset(abort_txns, 0, sizeof(int) * proxy->rw_node_thread_num_);

    server_start_time = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < proxy->rw_node_thread_num_; ++i) {
        std::cout << "i: " << i << "\n";
        proxy->rw_threads_.emplace_back([&rw_mutex_, &rw_cv_, &exception_thread_count, &recovered, i, proxy]() {
            try {
                run_client(proxy->bench_mark_, proxy->rw_node_ip_, proxy->rw_node_port_, i);
            } catch(ConnectionClosedException e) {
                std::unique_lock<std::mutex> lock(rw_mutex_);
                // exceptions[i] = std::current_exception();
                exception_thread_count ++;
                std::cout << "thread " << i << "encoutners failure, exception_thread_count is " << exception_thread_count << "\n";
                rw_cv_.wait(lock, [&recovered]{ return recovered; });
            }
            // std::cout << "thread id: " << i << ", after catch code\n";
            // reconnect_to_backup_rw(proxy->back_rw_ip_, proxy->back_rw_port_, i);
            // // {
            // //     std::lock_guard<std::mutex> lock(rw_mutex_);
            // //     if(std::all_of(exceptions.begin(), exceptions.end(), [](std::exception_ptr e) { return e != nullptr; })) {
            // //         all_threads_finished = true;
            // //         rw_cv_.notify_one();
            // //     }
            // // }
            // std::cout << "thread id: " << i << ", end\n";
        });
    }

    {
        // while(exception_thread_count < proxy->rw_node_thread_num_) {
        //     std::this_thread::sleep_for(std::chrono::seconds(1));
        // }
        
        // std::unique_lock<std::mutex> lock(rw_mutex_);   // unique_lock会在线程等待期间释放锁, lock_guard就没有这个功能了
        // rw_cv_.wait(lock, [&exception_thread_count, proxy] { return exception_thread_count == proxy->rw_node_thread_num_; });
        
        // notify_backup_rw_to_recover(proxy->back_rw_ip_, proxy->back_rw_port_);
        // recovered = true;
        // rw_cv_.notify_all();
    }

    // for(auto& e: exceptions) {
    //     if(e) {
    //         std::rethrow_exception(e);
    //         // catch(const ConnectionClosedException& e) {
    //         //     // std::cerr << 
    //         //     reconnect_to_backup_rw(proxy->back_rw_ip_, proxy->back_rw_port_);
    //         // }
    //     }
    // }

    for(auto& thread: proxy->rw_threads_) {
        thread.join();
    }

    server_end_time = std::chrono::high_resolution_clock::now();
    int64_t tot_duration = std::chrono::duration_cast<std::chrono::milliseconds>(server_end_time - server_start_time).count();

    int tot_commit_txn = 0;
    int tot_abort_txn = 0;

    for(int i = 0; i < proxy->rw_node_thread_num_; ++i) {
        tot_commit_txn += commit_txns[i];
        tot_abort_txn += abort_txns[i];
    }

    double commit_tput = (double)tot_commit_txn / ((double)tot_duration);
    double abort_tput = (double)tot_abort_txn / ((double)tot_duration);

    std::cout << "total throughput:\n";
    std::cout << "commit_tot_cnt: " << tot_commit_txn << ", time: " << tot_duration << "\n";
    std::cout << "commit_tput: " << commit_tput * 1000.0 << ", abort_tput: " << abort_tput << "\n";
}

int main(int argc, char** argv) {
    std::string config_path = "../src/config/proxy_config.json";

    cJSON* cjson = parse_json_file(config_path);
    cJSON* rw_node = cJSON_GetObjectItem(cjson, "rw_node");
    std::string workload = cJSON_GetObjectItem(rw_node, "workload")->valuestring;
    int record_num = cJSON_GetObjectItem(rw_node, "record_num")->valueint;
    int thread_num = cJSON_GetObjectItem(rw_node, "thread_num")->valueint;
    Proxy* proxy = new Proxy(workload, thread_num, record_num);
    client_num = thread_num;

    proxy->rw_node_ip_ = cJSON_GetObjectItem(rw_node, "ip")->valuestring;
    proxy->rw_node_port_ = cJSON_GetObjectItem(rw_node, "port")->valueint;

    cJSON* back_rw_node = cJSON_GetObjectItem(cjson, "back_rw_node");
    proxy->back_rw_ip_ = cJSON_GetObjectItem(back_rw_node, "ip")->valuestring;
    proxy->back_rw_port_ = cJSON_GetObjectItem(back_rw_node, "port")->valueint;

    run_proxy(proxy);
}