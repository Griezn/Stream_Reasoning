//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include <stdbool.h>

#include "source.h"
#include "data.h"


enum OPERATORS {
    JOIN,
    FILTER,
    WINDOW,
};


struct join_params {
    bool (*check)(triple_t in1, triple_t in2);
};

struct filter_params {
    bool (*check)(triple_t in);
};

struct window_params {
    unsigned char window_size;
};


typedef union Parameters {
    struct join_params   join;
    struct filter_params filter;
    struct window_params window;
} parameter_t;


typedef struct Operator {
    enum    OPERATORS   type;
    struct  Operator    *left;
    struct  Operator    *right;
    union   Parameters  params;
} operator_t;


typedef struct Query {
    struct Operator *root;
} query_t;


void execute_query(const query_t *query, source_t *source, sink_t *sink);

#endif //QUERY_H
