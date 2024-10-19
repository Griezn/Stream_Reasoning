//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


void join(const data_t *in1, const data_t *in2, data_t *out, const parameter_t param)
{
    out->data = malloc(in1->size * sizeof(int));
    out->size = in1->size;

    for (int i = 0; i < out->size; ++i) {
        if (param.join.check(in1->data[i], in2->data[i])) {
            out->data[i] = in1->data[i] - in2->data[i];
        } else {
            out->data[i] = in1->data[i] + in2->data[i];
        }
    }
}


void filter(const data_t *in, data_t *out, const parameter_t param)
{
    out->data = malloc(in->size * sizeof(int));
    out->size = in->size;

    for (int i = 0; i < in->size; ++i) {
        if (param.filter.check(in->data[i])) {
            out->data[i] = in->data[i] + 1;
        }
        else {
            out->data[i] = in->data[i] - 1;
        }
    }
}


void window(data_t *in, data_t *out, const parameter_t param)
{
    int size = min(in->size, param.window.window_size);
    out->data = malloc(size  * sizeof(int));
    out->size = size;

    in->size = in->size - size;
    memcpy(out->data, in->data, size * sizeof(int));
}


void execute_operator(const operator_t *operator_, data_t *in, data_t *out)
{
    data_t *tmpo1 = in;
    data_t *tmpo2 = NULL;

    switch (operator_->type) {
        case JOIN:
            assert(operator_->left);
            assert(operator_->right);
            tmpo1 = malloc(sizeof(data_t));
            tmpo2 = malloc(sizeof(data_t));

            execute_operator(operator_->left, in, tmpo1);
            execute_operator(operator_->right, in, tmpo2);

            assert(tmpo1);
            assert(tmpo2);
            join(tmpo1, tmpo2, out, operator_->params);
            free(tmpo2->data);
            free(tmpo2);
            break;
        case FILTER:
            if (operator_->left) {
                tmpo1 = malloc(sizeof(data_t));
                execute_operator(operator_->left, in, tmpo1);
            }

            assert(tmpo1);
            filter(tmpo1, out, operator_->params);
            break;
        case WINDOW:
            if (operator_->left) {
                tmpo1 = malloc(sizeof(data_t));
                execute_operator(operator_->left, in, tmpo1);
            }

            assert(tmpo1);
            window(tmpo1, out, operator_->params);
            break;
    }

    if (tmpo1 != in && tmpo1) {
        free(tmpo1->data);
        free(tmpo1);
    }
}


void execute_query(const query_t *query, source_t *source, sink_t *sink)
{
    data_t data = {NULL, 0};
    while (source->has_next) {
        execute_operator(query->root, source->get_next(source), &data);
        sink->push_next(sink, &data);
    }
}
