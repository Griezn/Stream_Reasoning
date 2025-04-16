//
// Created by Seppe Degryse on 15/04/2025.
//
#include <gtest/gtest.h>

extern "C"
{
#include "hash_table.h"
}

TEST(HashTests, test1)
{
    hash_table_t ht = create_table(10);

    triple_t triple1 = {1, 2, 3};
    triple_t triple12 = {1, 2, 23};
    triple_t triple2 = {2, 2, 3};
    insert(&ht, 2, &triple12);
    insert(&ht, 2, &triple1);

    ASSERT_TRUE(contains(&ht, 2, 2, &triple2) != nullptr);

    free_table(&ht);
}


TEST(HashTests, test2)
{
    hash_table_t ht = create_table(10);

    constexpr triple_t triple1 = {1, 2, 3};
    constexpr triple_t triple12 = {1, 4, 23};

    auto *buffer = static_cast<triple_t *>(malloc(sizeof(triple_t) * 2));
    buffer[0] = triple1;
    buffer[1] = triple12;

    triple_t triple2 = {2, 6, 413};
    triple_t triple22 = {2, 4, 23};
    auto *buffer2 = static_cast<triple_t *>(malloc(sizeof(triple_t) * 2));
    buffer2[0] = triple2;
    buffer2[1] = triple22;

    insert(&ht, 5, &buffer[0]);

    ASSERT_TRUE(contains(&ht, 5, 5, &buffer2[0]) != nullptr);

    free(buffer);
    free(buffer2);
    free_table(&ht);
}


TEST(HashTests, test3)
{
    hash_table_t ht = create_table(10);

    triple_t triple1 = {1, 2, 3};
    triple_t triple12 = {1, 4, 23};
    auto *buffer = static_cast<triple_t *>(malloc(sizeof(triple_t) * 2));
    buffer[0] = triple1;
    buffer[1] = triple12;

    triple_t triple11 = {11, 21, 31};
    triple_t triple112 = {1, 4, 23};
    auto *buffer1 = static_cast<triple_t *>(malloc(sizeof(triple_t) * 2));
    buffer1[0] = triple11;
    buffer1[1] = triple112;


    triple_t triple2 = {2, 6, 413};
    triple_t triple22 = {2, 4, 23};
    auto *buffer2 = static_cast<triple_t *>(malloc(sizeof(triple_t) * 2));
    buffer2[0] = triple2;
    buffer2[1] = triple22;

    insert(&ht, 5, &buffer[0]);
    insert(&ht, 5, &buffer1[0]);

    bucket_t *b = contains(&ht, 5, 5, &buffer2[0]);
    ASSERT_TRUE(b != nullptr);

    free(buffer);
    free(buffer1);
    free(buffer2);
    free_table(&ht);
}