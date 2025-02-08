//
// Created by Seppe Degryse on 08/12/2024.
//

#ifndef OPERATOR_H
#define OPERATOR_H
#include "source.h"

#include <stdint.h>
#include <stdbool.h>

enum OPERATORS {
    JOIN,
    FILTER,
    WINDOW,
    SELECT
};

typedef bool (*join_check_t)(triple_t in1, triple_t in2);

typedef bool (*filter_check_t)(triple_t in);

typedef struct JoinParams {
    uint8_t      size;
    join_check_t *checks;
} join_params_t;

typedef struct FilterParams {
    uint8_t size;
    filter_check_t *checks;
} filter_params_t;

typedef struct SelectParams {
    uint8_t size;
    uint8_t *colums;
} select_params_t;

typedef struct WindowParams {
    uint32_t size;
    uint32_t step;
    source_t *source;
} window_params_t;

typedef union Parameters {
    struct JoinParams    join;
    struct FilterParams  filter;
    struct SelectParams  select;
    struct WindowParams  window;
} parameter_t;

typedef struct Operator {
    enum    OPERATORS   type;
    struct  Operator    *left;
    struct  Operator    *right;
    union   Parameters  params;
} operator_t;

#endif //OPERATOR_H
