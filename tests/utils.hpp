//
// Created by Seppe Degryse on 18/10/2024.
//

#ifndef UTILS_HPP
#define UTILS_HPP
#include <gtest/gtest.h>

#define ASSERT_ARR_EQ(arr1, arr2, size)                     \
    for (size_t i = 0; i < size; ++i) {                     \
        ASSERT_EQ(arr1[i], arr2[i]) << "at index " << i;    \
    }

#endif //UTILS_HPP
