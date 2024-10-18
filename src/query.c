//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


void join(const data_t in1, const data_t in2, data_t *out, const parameter_t param)
{
    out->data = malloc(in1.size * sizeof(int));
    out->size = in1.size;

    for (int i = 0; i < out->size; ++i) {
        if (param.join.check(in1.data[i], in2.data[i])) {
            out->data[i] = in1.data[i] - in2.data[i];
        } else {
            out->data[i] = in1.data[i] + in2.data[i];
        }
    }
}


void filter(const data_t in, data_t *out, const parameter_t param)
{
    out->data = malloc(in.size * sizeof(int));
    out->size = in.size;

    for (int i = 0; i < in.size; ++i) {
        if (param.filter.check(in.data[i])) {
            out->data[i] = in.data[i] + 1;
        }
        else {
            out->data[i] = in.data[i] - 1;
        }
    }
}


void window(const data_t in, data_t *out, const parameter_t param)
{
    out->data = malloc(param.window.window_size * sizeof(int));
    out->size = param.window.window_size;
    memcpy(out->data, in.data, param.window.window_size * sizeof(int));
}


void execute_operator(const operator_t *operator_, const data_t in, data_t *out)
{
    assert(in.data);
    data_t tmpo1 = in;
    data_t tmpo2 = {NULL, 0};

    switch (operator_->type) {
        case JOIN:
            assert(operator_->left);
            assert(operator_->right);

            execute_operator(operator_->left, in, &tmpo1);
            execute_operator(operator_->right, in, &tmpo2);

            join(tmpo1, tmpo2, out, operator_->params);
            break;
        case FILTER:
            if (operator_->left)
                execute_operator(operator_->left, in, &tmpo1);

            filter(tmpo1, out, operator_->params);
            break;
        case WINDOW:
            if (operator_->left)
                execute_operator(operator_->left, in, &tmpo1);

            window(tmpo1, out, operator_->params);
            break;
    }

    if (tmpo1.data != in.data) {
        free(tmpo1.data);
    }
    if (tmpo2.data) {
        free(tmpo2.data);
    }
}


void execute_query(const query_t *query, source_t *source, const sink_t *sink)
{
    data_t data = {NULL, 0};
    while (source->has_next) {
        execute_operator(query->root, source->get_next(source), &data);
        sink->push_next(sink, data);
    }
}
