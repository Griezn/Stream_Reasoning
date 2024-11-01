//
// Created by Seppe Degryse on 29/10/2024.
//
#include "data.h"

void join_triple_copy(const data_t *src1, const unsigned char index1,
                        const data_t *src2, const unsigned char index2, data_t *dest)
{
    int index = dest->size * dest->width;
    for (int i = 0; i < src1->width; ++i) {
        dest->data[index++] = src1->data[index1 + i];
    }

    for (int i = 0; i < src2->width; ++i) {
        dest->data[index++] = src2->data[index2 + i];
    }

    dest->size++;
    dest->width = src1->width + src2->width;
}


bool join_check(const data_t *src1, const unsigned char index1,
                    const data_t *src2, const unsigned char index2, const join_check_t check)
{
    for (int i = 0; i < src1->width; ++i) {
        for (int j = 0; j < src2->width; ++j) {
            if (check(src1->data[index1 + i], src2->data[index2 + j]))
                return true;
        }
    }
    return false;
}


void triple_copy(const data_t *src, const unsigned char index, data_t *dest)
{
    int dest_index = dest->size * dest->width;
    for (int i = 0; i < src->width; ++i) {
        dest->data[dest_index++] = src->data[index + i];
    }

    dest->size++;
}


bool filter_check(const data_t *src, const unsigned char index, const filter_check_t check)
{
    for (int i = 0; i < src->width; ++i) {
        if (check(src->data[index + i]))
            return true;
    }
    return false;
}
