#include "buffer_allocator.h"

#include <queue>
#include <gtest/gtest.h>


/*
    only alloc, not free
*/
TEST(BufferAllocatorTest, AllocateTEST_1) {

    const size_t test_size = 1024;
    char *buffer = new char[test_size];
    RDMABufferAlloc allocator(buffer, buffer + test_size);

    for(size_t i = 0; i < test_size / 16; ++i) {

        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), 16 * i);
        EXPECT_EQ(allocator.getFreeSpace(), test_size - 16 * i);

        auto alloc_buffer  = allocator.Alloc(16);
        EXPECT_EQ(alloc_buffer.first, true);

        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), (16 * (i + 1)) % test_size);
        EXPECT_EQ(allocator.getFreeSpace(), test_size - 16 * (i + 1));
    }

    auto alloc_buffer = allocator.Alloc(16);
    EXPECT_EQ(alloc_buffer.first, false);
    EXPECT_EQ(allocator.get_curr_read_offset(), 0);
    EXPECT_EQ(allocator.get_curr_write_offset(), 0);
    EXPECT_EQ(allocator.getFreeSpace(), 0);

    // begin free
    for(size_t i = 0; i < test_size / 16; ++i) {
        allocator.Free(16);

        EXPECT_EQ(allocator.get_curr_read_offset(), (16 * (i + 1)) % test_size);
        EXPECT_EQ(allocator.get_curr_write_offset(), 0);
        EXPECT_EQ(allocator.getFreeSpace(), 16 * (i + 1));
    }
}

/*
    
*/
TEST(BufferAllocatorTest, AllocateTEST_2) {

    const size_t test_size = 1024;
    char *buffer = new char[test_size];
    RDMABufferAlloc allocator(buffer, buffer + test_size);

    int mod_size = test_size % 15;
    for(size_t i = 0; i < test_size / 15; ++i) {

        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), 15 * i);
        EXPECT_EQ(allocator.getFreeSpace(), test_size - 15 * i);

        auto alloc_buffer  = allocator.Alloc(15);
        EXPECT_EQ(alloc_buffer.first, true);
        

        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), (15 * (i + 1)) % test_size);
        EXPECT_EQ(alloc_buffer.second, buffer + (15 * i) % test_size);
        EXPECT_EQ(allocator.getFreeSpace(), test_size - 15 * (i + 1));
    }

    /*
        在alloc 15，将会失败
    */
    {
        auto alloc_buffer = allocator.Alloc(15);
        EXPECT_EQ(alloc_buffer.first, false);
        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), test_size - mod_size);
        EXPECT_EQ(allocator.getFreeSpace(), mod_size);
    }


    /*

    */
    {
        allocator.Free(15);
        EXPECT_EQ(allocator.get_curr_read_offset(), 15);
        EXPECT_EQ(allocator.get_curr_write_offset(), test_size - mod_size);
        EXPECT_EQ(allocator.getFreeSpace(), mod_size + 15);
    }
    /*

    */
    {
        auto alloc_buffer = allocator.Alloc(15);
        EXPECT_EQ(alloc_buffer.first, true);
        EXPECT_EQ(allocator.get_curr_read_offset(), 15);
        EXPECT_EQ(allocator.get_curr_write_offset(), 15);
        EXPECT_EQ(alloc_buffer.second, buffer);
        EXPECT_EQ(allocator.getFreeSpace(), 0);
    }

}

/*
    
*/
TEST(BufferAllocatorTest, RandomAlloc1) {

    const size_t test_size = 4096 * 10000;
    char *buffer = new char[test_size];
    RDMABufferAlloc allocator(buffer, buffer + test_size);

    size_t write_offset = 0;
    size_t read_offset = 0;
    size_t free_space = test_size;

    /*

    */
    std::queue<size_t> alloc_vector;

    for(int i = 0; i < 100000; ++i) {
        size_t size = rand() % test_size;
        auto alloc_buffer = allocator.Alloc(size);
        if(alloc_buffer.first) {
            size_t actual_alloc_offset = write_offset + size > test_size ? 0 : write_offset;

            write_offset = (write_offset + size) > test_size? size : (write_offset + size) % test_size;
            free_space = (test_size - write_offset + read_offset) % test_size;
            if(free_space == 0 && alloc_vector.empty()) {
                free_space = test_size;
            }


            EXPECT_EQ(allocator.get_curr_read_offset(), read_offset);
            EXPECT_EQ(allocator.get_curr_write_offset(), write_offset);
            EXPECT_EQ(alloc_buffer.second, buffer + actual_alloc_offset);
            EXPECT_EQ(allocator.getFreeSpace(), free_space);
            alloc_vector.push(size);
        } else {
            /*
                alloc不下来，alloc vector必不为空
            */
            EXPECT_GT(alloc_vector.size(), 0);

            EXPECT_EQ(allocator.get_curr_read_offset(), read_offset);
            EXPECT_EQ(allocator.get_curr_write_offset(), write_offset);
            EXPECT_EQ(allocator.getFreeSpace(), free_space);

            size_t alloc_size = alloc_vector.front();
            alloc_vector.pop();

            allocator.Free(alloc_size);
            read_offset = (read_offset + alloc_size) > test_size ? alloc_size : (read_offset + alloc_size) % test_size;
            free_space = (test_size - write_offset + read_offset) % test_size;
            if(free_space == 0 && alloc_vector.empty()) {
                free_space = test_size;
                read_offset = 0;
                write_offset = 0;
            }

            EXPECT_EQ(allocator.get_curr_read_offset(), read_offset);
            EXPECT_EQ(allocator.get_curr_write_offset(), write_offset);
            EXPECT_EQ(allocator.getFreeSpace(), free_space);
            
        }
    }
}


/*
    
*/
TEST(BufferAllocatorTest, CornerCase) {
    const size_t test_size = 1024;
    char buffer[test_size];
    RDMABufferAlloc allocator(buffer, buffer + test_size);

    {
        auto alloc_buffer = allocator.Alloc(0);
        EXPECT_EQ(alloc_buffer.first, false);
    }
    /*
        越界
    */
    {
        auto alloc_buffer = allocator.Alloc(1025);
        EXPECT_EQ(alloc_buffer.first, false);
    }

    /*

    */
    {
        auto alloc_buffer = allocator.Alloc(1024);
        EXPECT_EQ(alloc_buffer.first, true);
        EXPECT_EQ(alloc_buffer.second, buffer);

        EXPECT_EQ(allocator.get_curr_read_offset(), 0);
        EXPECT_EQ(allocator.get_curr_write_offset(), 0);
    }

}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}