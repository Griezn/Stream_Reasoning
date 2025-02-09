//
// Created by Seppe Degryse on 09/02/2025.
//

#ifndef DEFAULT_CHECKS_HPP
#define DEFAULT_CHECKS_HPP


extern "C" {
#include "data.h"
}

inline bool natural_join(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}


inline bool cartesian_join(const triple_t in1, const triple_t in2)
{
    return true;
}

#endif //DEFAULT_CHECKS_HPP
