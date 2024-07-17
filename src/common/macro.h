#include <cstdio>

#define print_char_array(arr, len) for(int i = 0; i < len; ++i) printf("%c", arr[i]); puts("");

#define print_bitmap(arr, len) for(int i = 0; i < len; ++i) for(int bit = 0; bit < 8; ++bit) { if(arr[i] & (1 << bit)) printf("1"); else printf("0"); }

#define print_bitmap_valid_bits(arr, len) for(int i = 0; i < len; ++i) for(int bit = 0; bit < 8; ++bit) { if(arr[i] & (1 << bit)) printf("%d, ", i*8+bit); }  puts("");