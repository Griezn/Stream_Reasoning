//
// Created by Seppe Degryse on 08/10/2024.
//
#include "query.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>


void join(int *in1, int *in2, int **out, parameter_t param)
{
    int *local_out = malloc(sizeof(int));
    *out = local_out;
    printf("The join funftion\n");
    if (param.join.check(in1, in2)) {
        printf("Join allowed\n");
    }
};


void filter(int *in, int **out, parameter_t param)
{
    int *local_out = malloc(sizeof(int));
    *out = local_out;

    if (param.filter.check(in)) {
        *local_out = (*in) + 1;
    }
}


void window(int *in, int **out, parameter_t param)
{
    int *local_out = malloc(sizeof(int));
    *out = local_out;
    printf("The window function, %p\n", in);
    printf("The window size is: %i\n", param.window.window_size);
}


void execute_operator(operator_t *operator_, int *in, int **out)
{
    assert(operator_);
    int *tmpo1 = in;
    int *tmpo2 = NULL;

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

    if (tmpo1 != in) {
        free(tmpo1);
    }
    if (tmpo2) {
        free(tmpo2);
    }
}


void execute_query(query_t *query, int *in, int **out)
{
    assert(query);
    execute_operator(query->root, in, out);
}
