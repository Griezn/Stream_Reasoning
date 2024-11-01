//
// Created by Seppe Degryse on 31/10/2024.
//

#ifndef DEFS_H
#define DEFS_H
#include <stdbool.h>

typedef struct Triple {
    unsigned char subject;
    unsigned char predicate;
    unsigned char object;
} triple_t;

typedef struct Data {
    struct Triple *data;
    unsigned char size;
    unsigned char width;
} data_t;

enum OPERATORS {
    JOIN,
    FILTER,
    WINDOW,
};

typedef bool (*join_check_t)(triple_t in1, triple_t in2);

typedef bool (*filter_check_t)(triple_t in);

typedef union Parameters {
    join_check_t   join;
    filter_check_t filter;
    unsigned char  window;
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

#endif //DEFS_H
