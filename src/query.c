//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"

#include <assert.h>
#include <stdlib.h>

//#define malloc(size) tracked_malloc(size)


void join(const data_t *in1, const data_t *in2, data_t *out, const join_params_t param)
{
    const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    out->size = 0;
    out->width = in1->width + in2->width;

    for (uint32_t i = 0; i < in1->size*in1->width; i += in1->width) {
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

    for (uint32_t i = 0; i < in1->size*in1->width; i += in1->width) {
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


bool execute_steps(const plan_t *plan, const int8_t from, const int8_t to)
{
    assert(from < to);

    for (int8_t i = to; i >= from; --i) {
        const step_t *step = &plan->steps[i]; assert(step);
        const operator_t *op = step->operator_; assert(op);
        const step_t *left_step = step->left_step;
        const step_t *right_step = step->right_step;
        data_t *output = step->output;

        switch (op->type) {
            case JOIN:
                assert(left_step->output); assert(right_step->output);
                join(left_step->output, right_step->output, output, op->params.join);
                if (op->left->type != WINDOW) free(left_step->output->data);
                if (op->right->type != WINDOW) free(right_step->output->data);
            break;

            case CARTESIAN:
                assert(left_step->output); assert(right_step->output);
                cart_join(left_step->output, right_step->output, output, op->params.cart_join);
                if (op->left->type != WINDOW) free(left_step->output->data);
                if (op->right->type != WINDOW) free(right_step->output->data);
            break;

            case FILTER:
                assert(left_step->output);
                filter(left_step->output, output, op->params.filter);
                if (op->left->type != WINDOW) free(left_step->output->data);
            break;

            case WINDOW:
                if (!window(output, op->params.window)) {
                    // cleanup
                    if (i+1 <= to) {
                        const step_t *last_step = &plan->steps[i+1]; assert(step);
                        const data_t *last_output = last_step->output; assert(last_output->data);
                        free(last_output->data);
                    }

                    return false;
                }
            break;

            case SELECT:
                assert(left_step->output);
                select_query(left_step->output, output, op->params.select);
                if (op->left->type != WINDOW) free(left_step->output->data);
            break;
        }
    }

    return true;
}


bool execute_plan(const plan_t *plan, data_t **out)
{
    const bool res = execute_steps(plan, 0, plan->num_steps-1);

    *out = plan->steps[0].output; assert(out); assert((*out)->data);
    return res;
}


void flatten_query(const operator_t* operator_, data_t *results, const uint8_t index, plan_t *plan)
{
    assert(operator_); assert(results); assert(plan);

    plan->num_steps++;

    if (operator_->left) flatten_query(operator_->left, results, index+1, plan);

    data_t *output = &results[index];
    step_t *left_step = operator_->left ? &plan->steps[index+1] : NULL;
    step_t *right_step = operator_->right ? &plan->steps[plan->num_steps] : NULL;

    plan->steps[index] = (step_t) {operator_, left_step, right_step, output,
        PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, false};

    if (operator_->right) flatten_query(operator_->right, results, plan->num_steps, plan);
}


void init_plan(plan_t *plan)
{
    plan->num_steps = 0;
    plan->num_threads = 0;
    plan->steps = calloc(MAX_OPERATOR_COUNT, sizeof(step_t)); assert(plan->steps);
}


void execute_query(const query_t *query, sink_t *sink)
{
    plan_t plan;
    init_plan(&plan);

    // Max number of operators (64)
    // 3: left input, right input, output
    data_t *results = calloc(MAX_OPERATOR_COUNT * 3, sizeof(data_t)); assert(results);

    flatten_query(query->root, results, 0, &plan);

    data_t *out = NULL;
    while (execute_plan(&plan, &out)) {
        sink->push_next(sink, out);
    }

    free(plan.steps);
    free(results);
}
