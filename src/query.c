//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"

#include <assert.h>

#include "buffer.h"
//#include "memory.h"

//#define malloc(size) tracked_malloc(size)
//#define realloc(ptr, size) tracked_realloc(ptr, size)


void join(const data_t *in1, const data_t *in2, data_t *out, const join_params_t param)
{
    INIT_BUFFER(out, in1->width + in2->width);

    for (uint32_t i = 0; i < in1->size * in1->width; i += in1->width) {
        for (uint32_t j = 0; j < in2->size * in2->width; j += in2->width) {
            if (join_check(in1, i, in2, j, param)) {
                join_triple_copy(in1, i, in2, j, out);
            }
        }
    }
}


void cart_join(const data_t *in1, const data_t *in2, data_t *out, const cart_join_params_t param)
{
    INIT_BUFFER(out, in1->width + in2->width);

    for (uint32_t i = 0; i < in1->size * in1->width; i += in1->width) {
        for (uint32_t j = 0; j < in2->size * in2->width; j += in2->width) {
            if (prob_check(param.probability)) {
                join_triple_copy(in1, i, in2, j, out);
            }
        }
    }
}


void filter(const data_t *in, data_t *out, const filter_params_t param)
{
    const uint32_t size = in->size * in->width;
    INIT_BUFFER(out, in->width);

    for (uint32_t i = 0; i < size; i += in->width) {
        if (filter_check(in, i, param)) {
            triple_copy(in, i, out);
        }
    }
}


bool window(data_t **out, const window_params_t params)
{
    data_t *data = params.source->get_next(params.source, params.size, params.step);

    if (data == NULL)
        return false;

    free(*out);
    *out = data;
    return true;
}


void select_query(const data_t *in, data_t *out, const select_params_t param)
{
    const uint32_t size = in->size * param.width;
    out->data = malloc(size * sizeof(triple_t));
    assert(out->data);
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


void *execute_step(void *args)
{
    step_t *step = args;
    while (!atomic_load(&step->quit)) {
        const operator_t *op = step->operator_;
        assert(op);
        step_t *left_step = step->left_step;
        step_t *right_step = step->right_step;
        spsc_queue_t *output_queue = step->output_queue;
        spsc_queue_t *left_queue = step->left_queue;
        spsc_queue_t *right_queue = step->right_queue;

        data_t *output = malloc(sizeof(data_t));
        data_t *left_input = NULL;
        data_t *right_input = NULL;

        if (left_step) {
            while (true) {
                if (spsc_dequeue(left_queue, &left_input)) break;
                if (atomic_load(&left_step->quit) && spsc_is_empty(left_queue)) {
                    atomic_store(&step->quit, true);
                    break;
                }
            }
        }
        if (right_step) {
            while (true) {
                if (spsc_dequeue(right_queue, &right_input)) break;
                if (atomic_load(&right_step->quit) && spsc_is_empty(right_queue)) {
                    atomic_store(&step->quit, true);
                    break;
                }
            }
        }
        if (atomic_load(&step->quit)) {
            free(output);
            goto skip;
        }

        switch (op->type) {
            case JOIN:
                assert(left_input);
                assert(right_input);
                join(left_input, right_input, output, op->params.join);
                break;

            case CARTESIAN:
                assert(left_input);
                assert(right_input);
                cart_join(left_input, right_input, output, op->params.cart_join);
                break;

            case FILTER:
                assert(left_input);
                filter(left_input, output, op->params.filter);
                break;

            case WINDOW:
                if (!window(&output, op->params.window)) {
                    atomic_store(&step->quit, true);
                    free(output);
                    return NULL;
                }
                break;

            case SELECT:
                assert(left_input);
                select_query(left_input, output, op->params.select);
                break;
        }

        spsc_enqueue(output_queue, output);

    skip:
        if (left_step) {
            if (op->left->type != WINDOW && left_input) {
                assert(left_input);
                free(left_input->data);
                left_input->data = NULL;
                free(left_input);
                left_input = NULL;
            } else {
                free(left_input);
                left_input = NULL;
            }
        }
        if (right_step) {
            if (op->right->type != WINDOW && right_input) {
                assert(right_input);
                free(right_input->data);
                right_input->data = NULL;
                free(right_input);
                right_input = NULL;
            } else {
                free(right_input);
                right_input = NULL;
            }
        }
    }

    return NULL;
}


void execute_plan_parallel(const plan_t *plan, pthread_t *threads)
{
    for (int i = 0; i < plan->num_steps; ++i) {
        pthread_create(&threads[i], NULL, execute_step, &plan->steps[i]);
    }
}


void execute_plan(const plan_t *plan)
{
    for (int i = plan->num_steps - 1; i >= 0; --i) {
        execute_step(&plan->steps[i]);
    }
}


void stop_plan(const plan_t *plan, const pthread_t *threads)
{
    for (int i = 0; i < plan->num_steps; ++i) {
        assert(threads[i]);
        pthread_join(threads[i], NULL);
    }
}


void flatten_query(const operator_t *operator_, spsc_queue_t *queues, const uint8_t index, plan_t *plan)
{
    assert(operator_);
    assert(queues);
    assert(plan);

    plan->num_steps++;

    if (operator_->left) flatten_query(operator_->left, queues, index + 1, plan);

    spsc_queue_t *output_queue = &queues[index];
    spsc_init(output_queue, 512);
    step_t *left_step = operator_->left ? &plan->steps[index + 1] : NULL;
    spsc_queue_t *left_queue = operator_->left ? &queues[index + 1] : NULL;
    step_t *right_step = operator_->right ? &plan->steps[plan->num_steps] : NULL;
    spsc_queue_t *right_queue = operator_->right ? &queues[plan->num_steps] : NULL;

    plan->steps[index] = (step_t){operator_, left_step, right_step, left_queue, right_queue, output_queue, false};

    if (operator_->right) flatten_query(operator_->right, queues, plan->num_steps, plan);
}


void init_plan(plan_t *plan)
{
    plan->num_steps = 0;
    plan->steps = calloc(MAX_OPERATOR_COUNT, sizeof(step_t));
    assert(plan->steps);
}


void free_queues(spsc_queue_t *queues, const size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        if (queues[i].buffer)
            spsc_destroy(&queues[i]);
    }
}


void execute_query(const query_t *query, sink_t *sink)
{
    plan_t plan;
    init_plan(&plan);

    // Max number of operators (64)
    spsc_queue_t *queues = calloc(MAX_OPERATOR_COUNT, sizeof(spsc_queue_t));
    assert(queues);

    flatten_query(query->root, queues, 0, &plan);

    pthread_t *threads = calloc(plan.num_steps, sizeof(pthread_t));

    execute_plan_parallel(&plan, threads);

    step_t *root = &plan.steps[0];
    while (true) {
        data_t *output;
        if (spsc_dequeue(root->output_queue, &output)) {
            sink->push_next(sink, output);
            free(output);
        } else if (atomic_load(&root->quit) && spsc_is_empty(root->output_queue)) break;
    }

    stop_plan(&plan, threads);

    free(threads);
    free(plan.steps);
    free_queues(queues, MAX_OPERATOR_COUNT);
    free(queues);
}
