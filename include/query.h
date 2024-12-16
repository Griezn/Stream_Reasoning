//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include "source.h"
#include "data.h"
#include "operator.h"

#include <stdint.h>



typedef struct Query {
    struct Operator *root;
} query_t;

typedef struct {
    const operator_t *operator_;
    const data_t *in;
    data_t *out;
} operator_thread_arg_t;


void execute_query(const query_t *query, sink_t *sink);

void join_triple_copy(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, data_t *dest);

bool join_check(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, join_params_t check);

void triple_copy(const data_t *src, uint32_t index, data_t *dest);

bool filter_check(const data_t *src, uint32_t index, filter_params_t check);

bool select_check(const data_t *src, uint32_t index, select_params_t param);

#endif //QUERY_H
