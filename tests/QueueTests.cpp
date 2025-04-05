//
// Created by Seppe Degryse on 30/03/2025.
//
#include <gtest/gtest.h>

extern "C"
{
#include "queue.h"
}

TEST(QueueTests, test1)
{
    spsc_queue_t queue;
    spsc_init(&queue, 512);

    ASSERT_TRUE(spsc_is_empty(&queue));

    data_t test = {nullptr, 1, 2, 3};
    ASSERT_TRUE(spsc_enqueue(&queue, &test));
    ASSERT_FALSE(spsc_is_empty(&queue));

    data_t *test2 = nullptr;
    ASSERT_TRUE(spsc_dequeue(&queue, &test2));
    ASSERT_EQ(test.size, (*test2).size);
    ASSERT_EQ(test.width, (*test2).width);
    ASSERT_EQ(test.cap, (*test2).cap);
    ASSERT_TRUE(spsc_is_empty(&queue));

    ASSERT_FALSE(spsc_dequeue(&queue, &test2));
    ASSERT_TRUE(spsc_is_empty(&queue));

    spsc_destroy(&queue);
}