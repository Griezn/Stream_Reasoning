//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"

#include <assert.h>
#include <stdlib.h>
#include <pthread.h>


/// The join operator
/// @param in1 The first input stream to be joined
/// @param in2 The second input stream to be joined
/// @param out The output stream containing matching from the first and hte second stream
/// @param param Join parameters containing a function ptr specifying the join condition
void join(const data_t *in1, const data_t *in2, data_t *out, const join_params_t param)
{
    const uint32_t size = (in1->size * in2->size) * (in1->width + in2->width);
    out->data = malloc(size * sizeof(triple_t));
    assert(out->data);
    out->size = 0;
    out->width = in1->width + in2->width;

    for (uint32_t i = 0; i < in1->size*in2->width; i += in1->width) {
        for (uint32_t j = 0; j < in2->size * in2->width; j += in2->width) {
            if (join_check(in1, i, in2, j, param)) {
                join_triple_copy(in1, i, in2, j, out);
            }
        }
    }
}


/// The filter operator
/// @param in The input stream to be filtered
/// @param out The filtered output stream
/// @param param The filter parameters containing a function ptr specifying the filter condition
/// @note The out buffer will likely be larger than the out->size
void filter(const data_t *in, data_t *out, const filter_params_t param)
{
    const uint32_t size = in->size * in->width;
    out->data = malloc(size * sizeof(triple_t));
    assert(out->data);
    out->size = 0;
    out->width = in->width;

    for (uint32_t i = 0; i < size; i += in->width) {
        if (filter_check(in, i, param)) {
            triple_copy(in, i, out);
        }
    }
}


/// The window operator creates a copy of the input stream in a newly specified size
/// @param out A selection of the input stream
/// @param params The window parameter
bool window(data_t *out, const window_params_t params)
{
    data_t* data = params.source->get_next(params.source, params.size, params.step, params.calls);

    if (data == NULL) {
        *params.quit = true;
        return false;
    }

    *out = *data;
    free(data);
    return true;
}


/// Performs a column selection on a stream
/// @param in The input stream
/// @param out The input stream with only the triples having one of the specified predicates
/// @param param The select parameter containing an array with the wanted predicates
void select_query(const data_t *in, data_t *out, const select_params_t param)
{
    const uint32_t size = in->size * param.size;
    out->data = malloc(size * sizeof(triple_t));
    out->size = in->size;
    out->width = param.size;
    uint32_t out_idx = 0;

    for (uint32_t i = 0; i < in->size; ++i) {
        for (uint32_t j = 0; j < in->width; ++j) {
            if (select_check(in, i * in->width + j, param)) {
                out->data[out_idx++] = in->data[i * in->width + j];
            }
        }
    }
}


bool execute_operator(const operator_t *operator_, const data_t *in, data_t *out);
void *execute_operator_thread(void *arg) {
    const operator_thread_arg_t *targ = arg;
    bool *return_value = malloc(sizeof(bool));
    *return_value = execute_operator(targ->operator_, targ->in, targ->out);
    return return_value;
}


/// This function executed the right operator
/// @param operator The operator to be executed
/// @param in The input stream
/// @param out The output stream
bool execute_operator(const operator_t *operator, const data_t *in, data_t *out)
{
    data_t tmpo1 = *in;
    data_t tmpo2 = {NULL, 0, 1};

    switch (operator->type) {
        case JOIN:
            assert(operator->left);
            assert(operator->right);

            bool left_bool = execute_operator(operator->left, in, &tmpo1);
            bool right_bool = execute_operator(operator->right, in, &tmpo2);

            if (!left_bool || !right_bool)
                return false;

            join(&tmpo1, &tmpo2, out, operator->params.join);

            free(tmpo2.data);
            tmpo2.data = NULL;
            break;
        case FILTER:
            if (operator->left) {
                if(!execute_operator(operator->left, in, &tmpo1))
                    return false;
            }

            filter(&tmpo1, out, operator->params.filter);
            break;
        case WINDOW:
            if (operator->left) {
                if(!execute_operator(operator->left, in, &tmpo1))
                    return false;
            }

            if (!window(out, operator->params.window))
                return false;

            break;
        case SELECT:
            if (operator->left) {
                if(!execute_operator(operator->left, in, &tmpo1))
                    return false;
            }

            select_query(&tmpo1, out, operator->params.select);
            break;
    }


    if (tmpo1.data != in->data) {
        assert(operator->left);
        if (operator->left->type == WINDOW)
            return true;

        free(tmpo1.data);
        tmpo1.data = NULL;
    }

    return true;
}


/// This function pulls the data out of the source, runs it through the query and sends it to the sink
/// @param query The query to be executed
/// @param source The source creating the input stream
/// @param sink The sink consuming the output stream
void execute_query(const query_t *query, const source_t *source, sink_t *sink)
{
    data_t data = {NULL, 0, 1};

    (void)source;
    while (!query->quit) {
        execute_operator(query->root, &data, &data);
        sink->push_next(sink, &data);
    }
}
