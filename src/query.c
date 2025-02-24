//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"
#include "memory.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

#define malloc(size) tracked_malloc(size)


void join(const data_t *in1, const data_t *in2, data_t *out, const join_params_t param)
{
    const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    out->size = 0;
    out->width = in1->width + in2->width;

    for (uint32_t i = 0; i < in1->size*in2->width; i += in1->width) {
        for (uint32_t j = 0; j < in2->size*in2->width; j += in2->width) {
            if (join_check(in1, i, in2, j, param)) {
                join_triple_copy(in1, i, in2, j, out);
            }
        }
    }
}


void cart_join(const data_t *in1, const data_t *in2, data_t *out, const cart_join_params_t param)
{
    const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    out->size = 0;
    out->width = in1->width + in2->width;

    for (uint32_t i = 0; i < in1->size*in2->width; i += in1->width) {
        for (uint32_t j = 0; j < in2->size*in2->width; j += in2->width) {
            if (prob_check(param.probability)) {
                join_triple_copy(in1, i, in2, j, out);
            }
        }
    }
}


void filter(const data_t *in, data_t *out, const filter_params_t param)
{
    const uint32_t size = in->size * in->width;
    out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    out->size = 0;
    out->width = in->width;

    for (uint32_t i = 0; i < size; i += in->width) {
        if (filter_check(in, i, param)) {
            triple_copy(in, i, out);
        }
    }
}


bool window(data_t *out, const window_params_t params)
{
    data_t* data = params.source->get_next(params.source, params.size, params.step);

    if (data == NULL)
        return false;

    *out = *data;
    free(data);
    return true;
}


void select_query(const data_t *in, data_t *out, const select_params_t param)
{
    // TODO: add extra test for double occurences in 1 row
    const uint32_t size = in->size * param.width;
    out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    out->size = in->size;
    out->width = param.width;
    uint32_t out_idx = 0;

    for (uint32_t i = 0; i < in->size; ++i) {
        for (uint32_t j = 0; j < in->width; ++j) {
            if (select_check(in, i * in->width + j, param)) {
                out->data[out_idx++] = in->data[i * in->width + j];
            }
        }
    }
}


bool execute_plan(const plan_t *plan, data_t **out)
{
    for (int8_t i = plan->num_steps - 1; i >= 0; --i) {
        if(plan->steps[i].operator_ == NULL) continue;
        const step_t *step = &plan->steps[i]; assert(step);
        const operator_t *op = step->operator_; assert(op);
        const data_t *left_input = step->left_input;
        const data_t *right_input = step->right_input;
        data_t *output = step->output;

        switch (op->type) {
            case JOIN:
                assert(left_input);
                assert(right_input);
                join(left_input, right_input, output, op->params.join);
                if (op->left->type != WINDOW) free(left_input->data);
                if (op->right->type != WINDOW) free(right_input->data);
            break;

            case CARTESIAN:
                assert(left_input);
                assert(right_input);
                cart_join(left_input, right_input, output, op->params.cart_join);
                if (op->left->type != WINDOW) free(left_input->data);
                if (op->right->type != WINDOW) free(right_input->data);
            break;

            case FILTER:
                assert(left_input);
                filter(left_input, output, op->params.filter);
                if (op->left->type != WINDOW) free(left_input->data);
            break;

            case WINDOW:
                if (!window(output, op->params.window)) return false;
            break;

            case SELECT:
                assert(left_input);
                select_query(left_input, output, op->params.select);
                if (op->left->type != WINDOW) free(left_input->data);
            break;
        }
    }

    *out = plan->steps[0].output; assert(out);
    return true;
}


void flatten_query(const operator_t* operator_, data_t *results, const uint8_t index, plan_t *plan)
{
    assert(operator_); assert(results); assert(plan);

    data_t *output = &results[index];

    const uint8_t next_index_l = 2*index + 1;
    const uint8_t next_index_r = 2*index + 2;
    data_t *left_input = operator_->left ? &results[next_index_l] : NULL;
    data_t *right_input = operator_->right ? &results[next_index_r] : NULL;

    const step_t step = {operator_, left_input, right_input, output};
    plan->steps[index] = step;
    plan->num_steps = MAX(plan->num_steps, index+1);

    if (operator_->left) flatten_query(operator_->left, results, next_index_l, plan);
    if (operator_->right) flatten_query(operator_->right, results, next_index_r, plan);
}


void init_plan(plan_t *plan)
{
    plan->num_steps = 0;
    plan->steps = calloc(MAX_OPERATOR_COUNT, sizeof(step_t)); assert(plan->steps);
}


/// This function pulls the data out of the source, runs it through the query and sends it to the sink
/// @param query The query to be executed
/// @param sink The sink consuming the output stream
void execute_query(const query_t *query, sink_t *sink)
{
    plan_t plan;
    init_plan(&plan);

    // Max number of operators (256)
    // 3: left input, right input, output
    data_t *results = malloc(MAX_OPERATOR_COUNT * 3 * sizeof(data_t)); assert(results);

    flatten_query(query->root, results, 0, &plan);

    data_t *out = NULL;
    while (execute_plan(&plan, &out)) {
        sink->push_next(sink, out);
    }

    free(plan.steps);
    free(results);
}
