#include "queue.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>

bool spsc_init(spsc_queue_t *q, size_t size)
{
    // Check if size is a power of 2
    if (size == 0 || (size & (size - 1)) != 0) {
        return false;
    }

    q->buffer = malloc(size * sizeof(void *));
    if (q->buffer == NULL) {
        return false;
    }

    q->mask = size - 1;
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
    return true;
}

void spsc_destroy(spsc_queue_t *q)
{
    free(q->buffer);
    q->buffer = NULL; // Prevent use-after-free
}

bool spsc_enqueue(spsc_queue_t *q, data_t *item)
{
    const size_t current_head = atomic_load_explicit(&q->head, memory_order_acquire);
    const size_t current_tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    // Check if queue is full
    if ((current_tail - current_head) == q->mask + 1) {
        return false;
    }

    q->buffer[current_tail & q->mask] = item;
    atomic_store_explicit(&q->tail, current_tail + 1, memory_order_release);
    return true;
}

bool spsc_dequeue(spsc_queue_t *q, data_t **item)
{
    const size_t current_head = atomic_load_explicit(&q->head, memory_order_acquire);
    const size_t current_tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    // Check if queue is empty
    if (current_head == current_tail) {
        return false;
    }

    *item = q->buffer[current_head & q->mask];
    atomic_store_explicit(&q->head, current_head + 1, memory_order_release);
    return true;
}

bool spsc_is_empty(spsc_queue_t *q)
{
    size_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    return head == tail;
}


void empty_queue(spsc_queue_t *q)
{
    data_t *output;
    while (!spsc_is_empty(q)) {
        spsc_dequeue(q, &output);
        free(output->data);
        free(output);
    }
}
