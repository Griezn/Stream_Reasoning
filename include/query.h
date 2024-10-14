//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include <stdbool.h>


enum OPERATORS {
    JOIN,
    FILTER,
    WINDOW
};


struct join_params {
    bool (*check)(int *in1, int *in2);
};

struct filter_params {
    bool (*check)(int *in);
};

struct window_params {
    int window_size;
};


typedef union Parameters {
    struct join_params join;
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


void execute_query(query_t *query, int *in, int **out);

#endif //QUERY_H
