#ifndef SPSC_QUEUE_LIBRARY_H
#define SPSC_QUEUE_LIBRARY_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>

#include "data.h"

typedef struct {
    _Alignas(64) atomic_size_t  head;
    _Alignas(64) atomic_size_t  tail;
    size_t                      mask;
    data_t                      **buffer;
} spsc_queue_t;

bool spsc_init(spsc_queue_t *q, size_t size);

void spsc_destroy(spsc_queue_t *q);

bool spsc_enqueue(spsc_queue_t *q, data_t *item);

bool spsc_dequeue(spsc_queue_t *q, data_t **item);

bool spsc_is_empty(spsc_queue_t *q);

#endif //SPSC_QUEUE_LIBRARY_H
