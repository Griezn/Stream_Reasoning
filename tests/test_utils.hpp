#ifndef UTILS_HPP
#define UTILS_HPP

extern "C" {
#include "data.h"
}

constexpr bool TRIP_EQ(const triple_t lhs, const triple_t rhs)
{
    return lhs.subject == rhs.subject &&
            lhs.predicate == rhs.predicate &&
            lhs.object == rhs.object;
}

constexpr bool ARR_EQ(const triple_t *lhs, const triple_t *rhs, const size_t size)
{
    for (int i = 0; i < size; ++i) {
        if(!TRIP_EQ(lhs[i], rhs[i]))
            return false;
    }
    return true;
}

#endif //UTILS_HPP
