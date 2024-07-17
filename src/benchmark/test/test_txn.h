#pragma once

#include <unordered_map>

#include "benchmark/native_txn.h"
#include "benchmark/util/random.h"

static std::string generate_name(int id) {
    char name[5];
    snprintf(name, sizeof(name), "%04d", id);
    return std::string(name);
}

static std::unordered_map<int, float> id_score_map;

// test_table(id int, name char(4), score float)
class TestTxn: public NativeTransaction {
public:
    int id;
    int record_num;

    TestTxn(int count) : record_num(count) {}

    void generate_new_txn() override {
        queries.clear();
        id = RandomGenerator::generate_random_int(1, record_num);
        std::string name = generate_name(id);
        float score = RandomGenerator::generate_random_float(1, 60);

        if(id_score_map.find(id) == id_score_map.end()) {
            std::cout << "id: " << id << ", expected score: 100\n";    
        }
        else {
            std::cout << "id: " << id << ", expected score: " << id_score_map[id] << "\n";
        }

        id_score_map[id] = score;

        std::string sql = "";
        queries.push_back("begin;");
        sql = "select score from test_table where id = " + std::to_string(id) + " and name = '" + name + "';";
        queries.push_back(sql);
        sql = "update test_table set score = " + std::to_string(score) + " where id = "  + std::to_string(id) + " and name = '" + name + "';";
        queries.push_back(sql);
        // sql = "select * from test_table where score <= 60.0;";
        // queries.push_back(sql);
        queries.push_back("commit;");
    }
};