#ifndef RANDOM_H
#define RANDOM_H

#include <ctime>
#include <cstdlib>

class RandomGenerator {
public:
    // xxxx-xx-xx
    static const int DATE_SIZE = 10;

    static void init() { srand(1219); }
    static int generate_random_int(int min, int max);
    static float generate_random_float(int min, int max);
    static void generate_random_str(char* str, int len);
    
    static void generate_random_region(char* str, int len);
    static void generate_random_numer_str(char* str, int len);
    static void generate_random_varchar(char* str, int min_len, int max_len);
    static void generate_randome_address(char* street_1, char* street_2, char* city, char* state, char* zip);
    static void generate_random_date(char* str);
    static void generate_date_from_idx(char* str, int idx);
    static void generate_random_mktsegment(char* str);
    static void generate_mktsegment_from_idx(char* str, int idx);
    static int NURand(int A, int x, int y);
    static void generate_random_lastname(int num, char* name);

    static int get_region_key_from_nation(char* nation);
    static void get_region_from_region_key(char* str, int len, int region_key);
    static void get_nation_from_region_nation_key(int region_key, int nation_key, char* nation);
};

#endif