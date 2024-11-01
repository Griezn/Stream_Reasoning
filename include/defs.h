//
// Created by Seppe Degryse on 31/10/2024.
//

#ifndef DEFS_H
#define DEFS_H
#include <stdbool.h>
#include <stdint.h>

typedef struct Triple {
    uint8_t subject;
    uint8_t predicate;
    uint8_t object;
} triple_t;

typedef struct Data {
    struct Triple *data;
    uint8_t size;
    uint8_t width;
} data_t;

enum OPERATORS {
    JOIN,
    FILTER,
    WINDOW,
};

typedef bool (*join_check_t)(triple_t in1, triple_t in2);

typedef bool (*filter_check_t)(triple_t in);

typedef union Parameters {
    join_check_t    join;
    filter_check_t  filter;
    uint8_t         window;
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
