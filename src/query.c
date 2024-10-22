//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"
#include "data.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


/// The join operator
/// @param in1 The first input stream to be joined
/// @param in2 The second input stream to be joined
/// @param out The output stream containing matching triples from the first stream
/// @param param Join parameters containing a function ptr specifying the join condition
/// @note The out buffer will likely be larger than the out->size
void join(const data_t *in1, const data_t *in2, data_t *out, const parameter_t param)
{
    const unsigned char size = max(in1->size, in2->size);
    out->data = malloc(size * sizeof(triple_t));
    out->size = 0;

    for (int i = 0; i < in1->size; ++i) {
        for (int j = 0; j < in2->size; ++j) {
            if (param.join.check(in1->data[i], in2->data[j])) {
                out->data[out->size++] = in1->data[i];
            }
        }
    }
}


/// The filter operator
/// @param in The input stream to be filtered
/// @param out The filtered output stream
/// @param param The filter parameters containing a function ptr specifying the filter condition
/// @note The out buffer will likely be larger than the out->size
void filter(const data_t *in, data_t *out, const parameter_t param)
{
    out->data = malloc(in->size * sizeof(triple_t));
    out->size = 0;

    for (int i = 0; i < in->size; ++i) {
        if (param.filter.check(in->data[i])) {
            out->data[out->size++] = in->data[i];
        }
    }
}


/// The window operator creates a copy of the input stream in a newly specified size
/// @param in The input stream
/// @param out A selection of the input stream
/// @param param The window parameter containing a size of the window
void window(data_t *in, data_t *out, const parameter_t param)
{
    const unsigned char size = min(in->size, param.window.window_size);
    out->data = malloc(size  * sizeof(triple_t));
    out->size = size;

    in->size = in->size - size;
    memcpy(out->data, in->data, size * sizeof(triple_t));
}


/// This function executed the right operator
/// @param operator_ The operator to be executed
/// @param in The input stream
/// @param out The output stream
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


/// This function pulls the data out of the source, runs it through the query and sends it to the sink
/// @param query The query to be executed
/// @param source The source creating the input stream
/// @param sink The sink consuming the output stream
void execute_query(const query_t *query, source_t *source, sink_t *sink)
{
    data_t data = {NULL, 0};
    while (source->has_next) {
        execute_operator(query->root, source->get_next(source), &data);
        sink->push_next(sink, &data);
    }
}
