//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef DATA_H
#define DATA_H
#include "defs.h"

#include <stdbool.h>

void join_triple_copy(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, data_t *dest);

bool join_check(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, join_params_t check);

void triple_copy(const data_t *src, uint32_t index, data_t *dest);

bool filter_check(const data_t *src, uint32_t index, filter_params_t check);

bool select_check(const data_t *src, uint32_t index, select_params_t param);

void free_data(data_t *data);

#endif //DATA_H
