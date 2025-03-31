//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"

#include <assert.h>

#include "buffer.h"
#include "memory.h"

#define malloc(size) tracked_malloc(size)
#define realloc(ptr, size) tracked_realloc(ptr, size)


void join(const data_t *in1, const data_t *in2, data_t *out, const join_params_t param)
{
    //const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    //out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    //out->size = 0;
    //out->width = in1->width + in2->width;
    INIT_BUFFER(out, in1->width + in2->width);

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
    //const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    //out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    //out->size = 0;
    //out->width = in1->width + in2->width;
    INIT_BUFFER(out, in1->width + in2->width);

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
    //out->data = malloc(size * sizeof(triple_t)); assert(out->data);
    //out->size = 0;
    //out->width = in->width;
    INIT_BUFFER(out, in->width);

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

/*
void handle_quit(const step_t *step)
{
    if (step->left_step) {
        step->left_step->quit = true;
        step->left_step->ready = false;
        pthread_cond_signal(&step->left_step->cond);
        pthread_mutex_unlock(&step->left_step->mutex);
        if (step->operator_->left->type != WINDOW && step->left_step->output->data != NULL)
            free(step->left_step->output->data);
    }
    if (step->right_step) {
        step->right_step->quit = true;
        step->right_step->ready = false;
        pthread_cond_signal(&step->right_step->cond);
        pthread_mutex_unlock(&step->right_step->mutex);
        if (step->operator_->right->type != WINDOW && step->right_step->output->data != NULL)
            free(step->right_step->output->data);
    }
}
*/

void execute_step(step_t *step)
{
    const operator_t *op = step->operator_; assert(op);
    step_t *left_step = step->left_step;
    step_t *right_step = step->right_step;
    spsc_queue_t *output_queue = step->output_queue;
    spsc_queue_t *left_queue = step->left_queue;
    spsc_queue_t *right_queue = step->right_queue;

    data_t *output = malloc(sizeof(data_t));
    data_t *left_input = NULL;
    data_t *right_input = NULL;

    if (left_step) {
        if (atomic_load(&left_step->quit)) {
            atomic_store(&step->quit, true);
            //step->quit = true;
            return;
        }
        spsc_dequeue(left_queue, &left_input);
        assert(left_input);
    }
    if (right_step) {
        if (atomic_load(&right_step->quit)) {
            atomic_store(&step->quit, true);
            //step->quit = true;
            goto skip;
        }
        spsc_dequeue(right_queue, &right_input);
        assert(right_input);
    }
    if (step->quit) goto skip;

    switch (op->type) {
        case JOIN:
            assert(left_input); assert(right_input);
            join(left_input, right_input, output, op->params.join);
        break;

        case CARTESIAN:
            assert(left_input); assert(right_input);
            cart_join(left_input, right_input, output, op->params.cart_join);
        break;

        case FILTER:
            assert(left_input);
            filter(left_input, output, op->params.filter);
        break;

        case WINDOW:
            if (!window(output, op->params.window)) {
                atomic_store(&step->quit, true);
                return;
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
        assert(left_input);
        if (op->left->type != WINDOW) {free(left_input->data); left_input->data = NULL;}
    }
    if (right_step) {
        assert(right_input);
        if (op->right->type != WINDOW) {free(right_input->data); right_input->data = NULL;}
    }
}

/*
void *execute_step_parallel(void *arg)
{
    step_t* step = arg;

    while (true) {
        const operator_t *op = step->operator_; assert(op);
        step_t *left_step = step->left_step;
        step_t *right_step = step->right_step;
        data_t *output = step->output;

        pthread_mutex_lock(&step->mutex);
        while (step->ready) {pthread_cond_wait(&step->cond, &step->mutex);}

        // WAIT FOR CHILD(REN)
        if (left_step) {
            pthread_mutex_lock(&left_step->mutex);
            while (!left_step->ready) {pthread_cond_wait(&left_step->cond, &left_step->mutex);}
            if (left_step->quit) {
                step->quit = true;
            }
        }
        if (right_step) {
            pthread_mutex_lock(&right_step->mutex);
            while (!right_step->ready) {pthread_cond_wait(&right_step->cond, &right_step->mutex);}
            if (right_step->quit) {
                step->quit = true;
            }
        }
        if (step->quit) break;

        switch (op->type) {
            case JOIN:
                assert(left_step->output); assert(right_step->output);
                join(left_step->output, right_step->output, output, op->params.join);
            break;

            case CARTESIAN:
                assert(left_step->output); assert(right_step->output);
                cart_join(left_step->output, right_step->output, output, op->params.cart_join);
            break;

            case FILTER:
                assert(left_step->output);
                filter(left_step->output, output, op->params.filter);
            break;

            case WINDOW:
                if (!window(output, op->params.window)) {
                    step->quit = true;
                    goto quit;
                }
            break;

            case SELECT:
                assert(left_step->output);
                select_query(left_step->output, output, op->params.select);
            break;
        }

        step->ready = true;
        pthread_cond_signal(&step->cond);
        pthread_mutex_unlock(&step->mutex);

        if (left_step) {
            if (op->left->type != WINDOW) {free(left_step->output->data); left_step->output->data = NULL;}
            left_step->ready = false;
            pthread_cond_signal(&left_step->cond);
            pthread_mutex_unlock(&left_step->mutex);
        }
        if (right_step) {
            if (op->right->type != WINDOW) {free(right_step->output->data); right_step->output->data = NULL;}
            right_step->ready = false;
            pthread_cond_signal(&right_step->cond);
            pthread_mutex_unlock(&right_step->mutex);
        }
    }

    quit:
    handle_quit(step);
    step->ready = true;
    pthread_cond_signal(&step->cond);
    pthread_mutex_unlock(&step->mutex);
    return NULL;
}


void execute_plan_parallel(const plan_t *plan, pthread_t *threads)
{
    for (int i = 0; i < plan->num_steps; ++i) {
        pthread_create(&threads[i], NULL, execute_step_parallel, &plan->steps[i]);
    }
}
*/


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


void flatten_query(const operator_t* operator_, spsc_queue_t *queues, const uint8_t index, plan_t *plan)
{
    assert(operator_); assert(queues); assert(plan);

    plan->num_steps++;

    if (operator_->left) flatten_query(operator_->left,  queues, index+1, plan);

    spsc_queue_t *output_queue = &queues[index];
    spsc_init(output_queue, 512);
    step_t *left_step = operator_->left ? &plan->steps[index+1] : NULL;
    spsc_queue_t *left_queue = operator_->left ? &queues[index+1] : NULL;
    step_t *right_step = operator_->right ? &plan->steps[plan->num_steps] : NULL;
    spsc_queue_t *right_queue = operator_->right ? &queues[plan->num_steps] : NULL;

    plan->steps[index] = (step_t) {operator_, left_step, right_step, left_queue, right_queue, output_queue, false};

    if (operator_->right) flatten_query(operator_->right, queues, plan->num_steps, plan);
}


void init_plan(plan_t *plan)
{
    plan->num_steps = 0;
    plan->steps = calloc(MAX_OPERATOR_COUNT, sizeof(step_t)); assert(plan->steps);
}


void execute_query(const query_t *query, sink_t *sink)
{
    plan_t plan;
    init_plan(&plan);

    // Max number of operators (64)
    spsc_queue_t *results = calloc(MAX_OPERATOR_COUNT, sizeof(spsc_queue_t)); assert(results);

    flatten_query(query->root, results, 0, &plan);

    step_t *root = &plan.steps[0];
    while (!atomic_load(&root->quit)) {
        execute_plan(&plan);

        if (!atomic_load(&root->quit)) {
            data_t *output;
            spsc_dequeue(root->output_queue, &output);
            sink->push_next(sink, output);
        }
    }

    free(plan.steps);
    free(results);
}


/*
void execute_query_parallel(const query_t *query, sink_t *sink)
{
    plan_t plan;
    init_plan(&plan);

    // Max number of operators (64)
    data_t *queues = calloc(MAX_OPERATOR_COUNT, sizeof(data_t)); assert(results);

    flatten_query(query->root, queues, 0, &plan);

    pthread_t *threads = calloc(plan.num_steps, sizeof(pthread_t));

    execute_plan_parallel(&plan, threads);

    step_t *root = &plan.steps[0];
    while (true) {
        pthread_mutex_lock(&root->mutex);
        while (!root->ready) {pthread_cond_wait(&root->cond, &root->mutex);}

        if (root->quit) break;

        sink->push_next(sink, root->output);

        root->ready = false;
        pthread_cond_signal(&root->cond);
        pthread_mutex_unlock(&root->mutex);
    }

    stop_plan(&plan, threads);

    free(threads);
    free(plan.steps);
    free(results);
}
*/
