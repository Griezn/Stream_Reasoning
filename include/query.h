//
// Created by Seppe Degryse on 08/10/2024.
//
#ifndef QUERY_H
#define QUERY_H
#include "source.h"
#include "data.h"
#include "operator.h"
#include "queue.h"

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>


typedef struct ExecutionStep {
    const operator_t *operator_;
    struct ExecutionStep *left_step;
    struct ExecutionStep *right_step;
    spsc_queue_t *left_queue;
    spsc_queue_t *right_queue;
    spsc_queue_t *output_queue;
    atomic_bool quit;
} step_t;

#define UNPACK_STEP_FIELDS(step)                      \
    const operator_t *op = step->operator_;           \
    step_t *left_step = (step)->left_step;            \
    step_t *right_step = (step)->right_step;          \
    spsc_queue_t *output_queue = (step)->output_queue;\
    spsc_queue_t *left_queue = (step)->left_queue;    \
    spsc_queue_t *right_queue = (step)->right_queue;


#define UNPACK_STEP_FIELDS_WO_QUEUES(step)            \
    const operator_t *op = step->operator_;           \
    step_t *left_step = (step)->left_step;            \
    step_t *right_step = (step)->right_step;          \


#define MAX_OPERATOR_COUNT 64 //uint8_t

typedef struct ExecutionPlan {
    struct ExecutionStep *steps;
    uint8_t num_steps;
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

void flatten_query(const operator_t* operator_, spsc_queue_t *results, uint8_t index, plan_t *plan);

void join_triple_copy(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, data_t *dest);

bool join_check(const data_t *src1, uint32_t index1,
                    const data_t *src2, uint32_t index2, join_params_t check);

void triple_copy(const data_t *src, uint32_t index, data_t *dest);

bool filter_check(const data_t *src, uint32_t index, filter_params_t check);

bool select_check(const data_t *src, uint32_t index, select_params_t param);

bool prob_check(double probability);

#endif //QUERY_H
