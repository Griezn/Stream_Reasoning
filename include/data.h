//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef DATA_H
#define DATA_H
#include "defs.h"

#include <stdbool.h>

void join_triple_copy(const data_t *src1, unsigned char index1,
                    const data_t *src2, unsigned char index2, data_t *dest);

bool join_check(const data_t *src1, unsigned char index1,
                    const data_t *src2, unsigned char index2, join_check_t check);

void triple_copy(const data_t *src, unsigned char index, data_t *dest);

bool filter_check(const data_t *src, unsigned char index, filter_check_t check);


#endif //DATA_H
