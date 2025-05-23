//
// Created by Seppe Degryse on 29/10/2024.
//
#include "data.h"
#include "operator.h"
#include "buffer.h"

#include <assert.h>
#include <stdlib.h>

#include "hash_table.h"
#include "memory.h"

#define malloc(size) malloc(size)
#define realloc(ptr, size) realloc(ptr, size)

void join_triple_copy(const data_t *src1, const uint32_t index1,
                        const data_t *src2, const uint32_t index2, data_t *dest)
{
    uint32_t index = dest->size * dest->width;
    for (uint32_t i = 0; i < src1->width; ++i) {
        PUSH_TO_BUFFER(dest, index++, src1->data[index1 + i]);
        //dest->data[index++] = src1->data[index1 + i];
    }

    for (uint32_t i = 0; i < src2->width; ++i) {
        PUSH_TO_BUFFER(dest, index++, src2->data[index2 + i]);
        //dest->data[index++] = src2->data[index2 + i];
    }

    dest->size++;
}


void join_bucket_copy(const data_t *src1, bucket_t *bucket,
                    const data_t *src2, uint32_t index2, data_t *dest)
{
    uint32_t index = dest->size * dest->width;
    for (size_t j = 0; j < bucket->count; ++j) {
        for (uint32_t i = 0; i < src1->width; ++i) {
            //PUSH_TO_BUFFER(dest, index++, el1[i]);
            PUSH_TO_BUFFER(dest, index++, bucket->tuples[j][i]);
            //dest->data[index++] = src1->data[index1 + i];
        }

        for (uint32_t i = 0; i < src2->width; ++i) {
            PUSH_TO_BUFFER(dest, index++, src2->data[index2 + i]);
            //dest->data[index++] = src2->data[index2 + i];
        }

        dest->size++;
    }
}


bool join_check(const data_t *src1, const uint32_t index1,
                    const data_t *src2, const uint32_t index2, const join_params_t check)
{
    uint8_t checks_passed = 0;
    for (int k = 0; k < check.size; ++k) {
        bool match_found = false;
        // loop over all triple combinations
        for (uint32_t i = 0; i < src1->width && !match_found; ++i) {
            for (uint32_t j = 0; j < src2->width && !match_found; ++j) {
                if (check.checks[k](src1->data[index1 + i], src2->data[index2 + j])) {
                    checks_passed++;
                    match_found = true;
                }
            }
        }
        if (!match_found)
            return false;
    }
    return checks_passed == check.size;
}


void triple_copy(const data_t *src, const uint32_t index, data_t *dest)
{
    uint32_t dest_index = dest->size * dest->width;
    for (uint32_t i = 0; i < src->width; ++i) {
        PUSH_TO_BUFFER(dest, dest_index++, src->data[index + i]);
        //dest->data[dest_index++] = src->data[index + i];
    }

    dest->size++;
}


bool filter_check(const data_t *src, const uint32_t index, const filter_params_t check)
{
    uint8_t checks_passed = 0;
    for (int k = 0; k < check.size; ++k) {
        bool triple_found = false;
        for (uint32_t i = 0; i < src->width && !triple_found; ++i) {
            if (check.checks[k](src->data[index + i])) {
                checks_passed++;
                triple_found = true;
            }
        }
        if (!triple_found)
            return false;
    }
    return checks_passed == check.size;
}


bool select_check(const data_t *src, const uint32_t index, const select_params_t param)
{
    for (int i = 0; i < param.size; ++i) {
        if (src->data[index].predicate == param.colums[i])
            return true;
    }
    return false;
}


bool prob_check(const double probability)
{
    return (double) rand() / (double) RAND_MAX < probability;
}


void free_data(data_t *data)
{
    assert(data);
    assert(data->data);

    free(data->data);
    data->data = NULL;

    free(data);
    data = NULL;
}
