#ifndef NATIVE_TXN_H
#define NATIVE_TXN_H

#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>
#include <cstring>

class NativeTransaction {
public:
    void init() {}
    void clear() { queries.clear(); received_results_count = 0; }
    virtual void generate_new_txn() = 0;
    std::vector<std::string> queries;
    int received_results_count;

    void print_queries() {
        for(int i = 0; i < queries.size(); ++i) std::cout << queries[i] << std::endl;
    }
};

#endif