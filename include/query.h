//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include "source.h"
#include "defs.h"

typedef struct {
    const operator_t *operator_;
    const data_t *in;
    data_t *out;
} operator_thread_arg_t;


void execute_query(const query_t *query, const source_t *source, sink_t *sink);

#endif //QUERY_H
