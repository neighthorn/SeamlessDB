#include "random.h"
#include <cstring>
#include <string>
#include <cassert>
#include <iostream>

int RandomGenerator::generate_random_int(int min, int max) {
    int rand_int;
    int rand_range = max - min + 1;
    rand_int = rand() % rand_range;
    rand_int += min;
    return rand_int;
}

float RandomGenerator::generate_random_float(int min, int max) {
    // int base_num = rand() % 1219 + 10;
    // min *= base_num;
    // max *= base_num;
    // return (float) generate_random_int(min, max) / (float) base_num;
    float num_poll[5] = { 0.625, 0.125, 0.5, 0.25, 0.3125 };
    float base_num = (float)generate_random_int(min, max - 1);
    int index = rand() % 5;
    base_num += num_poll[index];
    return base_num;
}

void RandomGenerator::generate_random_str(char* str, int len) {
    static char *alphabets = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static int alphabet_num = 61;
    for(int i = 0; i < len; ++i)
        str[i] = alphabets[generate_random_int(0, alphabet_num)];
    // str[len] = '\0';
}

int RandomGenerator::get_region_key_from_nation(char* nation) {
    if(strstr(nation, "ALGERIA") != nullptr || strstr(nation, "ETHIOPIA") != nullptr || strstr(nation, "KENYA") != nullptr
        || strstr(nation, "MOROCCO") != nullptr || strstr(nation, "MOZAMBIQUE") != nullptr) {
        return 0;
    }
    if(strstr(nation, "ARGENTINA") != nullptr || strstr(nation, "BRAZIL") != nullptr || strstr(nation, "CANADA") != nullptr
        || strstr(nation, "PERU") != nullptr || strstr(nation, "UNITED STATES") != nullptr) {
        return 1;
    }
    if(strstr(nation, "INDIA") != nullptr || strstr(nation, "INDONESIA") != nullptr || strstr(nation, "JAPAN") != nullptr
        || strstr(nation, "CHINA") != nullptr || strstr(nation, "VIETNAM") != nullptr) {
        return 2;
    }
    if(strstr(nation, "FRANCE") != nullptr || strstr(nation, "GERMANY") != nullptr || strstr(nation, "ROMANIA") != nullptr
        || strstr(nation, "RUSSIA") != nullptr || strstr(nation, "UNITED KINGDOM") != nullptr) {
        return 3;
    }
    if(strstr(nation, "EGYPT") != nullptr || strstr(nation, "IRAN") != nullptr || strstr(nation, "IRAQ") != nullptr
        || strstr(nation, "JORDAN") != nullptr || strstr(nation, "SAUDI ARABIA") != nullptr) {
        return 4;
    }
}

void RandomGenerator::get_nation_from_region_nation_key(int region_key, int nation_key, char* nation) {
    // 根据 region_key 选择国家
    switch (region_key) {
        case 0: // 非洲区域
            switch (nation_key) {
                case 1: strcpy(nation, "ALGERIA"); return;
                case 2: strcpy(nation, "ETHIOPIA"); return;
                case 3: strcpy(nation, "KENYA"); return;
                case 4: strcpy(nation, "MOROCCO"); return;
                case 5: strcpy(nation, "MOZAMBIQUE"); return;
            }
            break;
        case 1: // 美洲区域
            switch (nation_key) {
                case 1: strcpy(nation, "ARGENTINA"); return;
                case 2: strcpy(nation, "BRAZIL"); return;
                case 3: strcpy(nation, "CANADA"); return;
                case 4: strcpy(nation, "PERU"); return;
                case 5: strcpy(nation, "UNITED STATES"); return;
            }
            break;
        case 2: // 亚洲区域
            switch (nation_key) {
                case 1: strcpy(nation, "INDIA"); return;
                case 2: strcpy(nation, "INDONESIA"); return;
                case 3: strcpy(nation, "JAPAN"); return;
                case 4: strcpy(nation, "CHINA"); return;
                case 5: strcpy(nation, "VIETNAM"); return;
            }
            break;
        case 3: // 欧洲区域
            switch (nation_key) {
                case 1: strcpy(nation, "FRANCE"); return;
                case 2: strcpy(nation, "GERMANY"); return;
                case 3: strcpy(nation, "ROMANIA"); return;
                case 4: strcpy(nation, "RUSSIA"); return;
                case 5: strcpy(nation, "UNITED KINGDOM"); return;
            }
            break;
        case 4: // 中东区域
            switch (nation_key) {
                case 1: strcpy(nation, "EGYPT"); return;
                case 2: strcpy(nation, "IRAN"); return;
                case 3: strcpy(nation, "IRAQ"); return;
                case 4: strcpy(nation, "JORDAN"); return;
                case 5: strcpy(nation, "SAUDI ARABIA"); return;
            }
            break;
        default:
            strcpy(nation, "Invalid region key");
            return;
    }

    strcpy(nation, "Invalid region key or nation key");
}

void RandomGenerator::generate_random_region(char* str, int len) {
    int region_key = generate_random_int(0, 4);
    get_region_from_region_key(str, len, region_key);
}

void RandomGenerator::get_region_from_region_key(char* str, int len, int region_key) {
    memset(str, 0, len);

    switch(region_key) {
        case 0: {
            memcpy(str, "AFRICA", 6);
        } break;
        case 1: {
            memcpy(str, "AMERICA", 7);
        } break;
        case 2: {
            memcpy(str, "ASIA", 4);
        } break;
        case 3: {
            memcpy(str, "EUROPE", 6);
        } break;
        case 4: {
            memcpy(str, "MIDDLE EAST", 11);
        } break;
        default: 
            generate_random_str(str, len);
            break;
    }
}

void RandomGenerator::generate_random_numer_str(char* str, int len) {
    for(int i = 0; i < len; ++i)
        str[i] = generate_random_int(0, 9) + '0';
    // str[len] = 0;
}

void RandomGenerator::generate_random_varchar(char* str, int min_len, int max_len) {
    generate_random_str(str, generate_random_int(min_len, max_len));
}

void RandomGenerator::generate_randome_address(char* street_1, char* street_2, char* city, char* state, char* zip) {
    // generate_random_varchar(street_1, 10, 20);
    generate_random_str(street_1, 20);
    // generate_random_varchar(street_2, 10, 20);
    generate_random_str(street_2, 20);
    // generate_random_varchar(city, 10, 20);
    generate_random_str(city, 20);
    generate_random_varchar(state, 2, 2);
    generate_random_varchar(zip, 9, 9);
}

int RandomGenerator::NURand(int A, int x, int y) {
    static int first = 1;
    unsigned C, C_255, C_1023, C_8191;

    if(first) {
        C_255 = generate_random_int(0, 255);
        C_1023 = generate_random_int(0, 1023);
        C_8191 = generate_random_int(0, 8191);
        first = 0;
    }

    switch(A) {
        case 255: C = C_255; break;
        case 1023: C = C_1023; break;
        case 8191: C = C_8191; break;
        default: break;
    }

    return (int)(((generate_random_int(0, A) | generate_random_int(x, y)) + C) % (y - x + 1)) + x;
}

void RandomGenerator::generate_random_lastname(int num, char* name) {
    static char *n[] = 
    {"BARR", "OUGH", "ABLE", "PRII", "PRES", 
     "ESEE", "ANTI", "CALL", "ATIO", "EING"};

    strcpy(name,n[num/100]);
    strcat(name,n[(num/10)%10]);
    strcat(name,n[num%10]);
}

void RandomGenerator::generate_random_date(char* str) {
    memset(str, 0, DATE_SIZE);
    // 1992-xx-01 ~ 1998-xx-01
    int year_end = generate_random_int(2, 8);
    std::string date = "199";
    date += std::to_string(year_end) + "-";
    int month = generate_random_int(1, 12);
    if(month < 10)
        date += "0" + std::to_string(month) + "-01";
    else
        date += std::to_string(month) + "-01";
    assert(date.length() == DATE_SIZE);
    memcpy(str, date.c_str(), DATE_SIZE);
    str[DATE_SIZE] = '\0';
}

void RandomGenerator::generate_date_from_idx(char* str, int idx) {
    memset(str, 0, DATE_SIZE);
    int year_end = idx / 12 + 2;
    std::string date = "199" + std::to_string(year_end) + "-";
    int month = idx % 12 + 1;
    if(month < 10)
        date += "0" + std::to_string(month) + "-01";
    else
        date += std::to_string(month) + "-01";
    assert(date.length() == DATE_SIZE);
    memcpy(str, date.c_str(), DATE_SIZE);
    date[DATE_SIZE] = '\0';
}

void RandomGenerator::generate_random_mktsegment(char* str) {
    static std::string mktsegment[] = {"AUTOMOBILE", "BUILDINGBU", "FURNITUREF", "MACHINERYM", "HOUSEHOLDH"};
    int idx = generate_random_int(0, 4);
    strncpy(str, mktsegment[idx].c_str(), 10);
    str[10] = '\0';
}

void RandomGenerator::generate_mktsegment_from_idx(char* str, int idx) {
    static std::string mktsegment[] = {"AUTOMOBILE", "BUILDINGBU", "FURNITUREF", "MACHINERYM", "HOUSEHOLDH"};
    strncpy(str, mktsegment[idx].c_str(), 10);
    str[10] = '\0';
}