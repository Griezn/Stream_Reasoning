//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include "source.h"
#include "data.h"
#include "operator.h"

#include <stdint.h>
#include <pthread.h>


typedef struct ExecutionStep {
    const operator_t *operator_;
    struct ExecutionStep *left_step;
    struct ExecutionStep *right_step;
    data_t *output;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool ready;
} step_t;

#define MAX_OPERATOR_COUNT 64 //uint8_t

typedef struct ExecutionPlan {
    struct ExecutionStep *steps;
    uint8_t num_steps;
    uint8_t num_threads;
} plan_t;

typedef struct Query {
    struct Operator *root;
} query_t;

typedef struct {
    const operator_t *operator_;
    const data_t *in;
    data_t *out;
} operator_thread_arg_t;


void execute_query(const query_t *query, sink_t *sink);

void flatten_query(const operator_t* operator_, data_t *results, uint8_t index, plan_t *plan);

void join_triple_copy(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, data_t *dest);

bool join_check(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, join_params_t check);

void triple_copy(const data_t *src, uint32_t index, data_t *dest);

bool filter_check(const data_t *src, uint32_t index, filter_params_t check);

bool select_check(const data_t *src, uint32_t index, select_params_t param);

bool prob_check(double probability);

#endif //QUERY_H
