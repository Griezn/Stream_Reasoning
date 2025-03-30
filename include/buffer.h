//
// Created by Seppe Degryse on 26/03/2025.
//

#ifndef BUFFER_H
#define BUFFER_H

#define INITIAL_BUFFER_CAPACITY 8

#define INIT_BUFFER(out, _width)              \
    do {                                     \
        (out)->data = malloc(INITIAL_BUFFER_CAPACITY * (_width) * sizeof(triple_t)); \
        assert((out)->data);                 \
        (out)->size = 0;                     \
        (out)->width = (_width);              \
        (out)->cap = INITIAL_BUFFER_CAPACITY * (_width);\
    } while (0)

#define PUSH_TO_BUFFER(out, index, element)                    \
    do {                                                       \
        if ((out)->size * (out)->width >= (out)->cap) {        \
            (out)->cap *= 2;                                   \
            (out)->data = realloc((out)->data, (out)->cap * (out)->width * sizeof(triple_t)); \
            assert((out)->data);                               \
        }                                                      \
        (out)->data[index] = (element);                        \
    } while (0)

#endif //BUFFER_H
