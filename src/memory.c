//
// Created by Seppe Degryse on 22/02/2025.
//
#include "memory.h"
#include <stdlib.h>

static size_t total_allocated = 0;
static size_t allocation_count = 0;
static size_t current = 0;
static size_t peak_allocated = 0;


void *tracked_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (ptr) {
        total_allocated += size;
        current += size;
        if (current > peak_allocated) peak_allocated = current;
        allocation_count++;
    }
    return ptr;
}


void *tracked_realloc(void *ptr, size_t new_size)
{
    total_allocated -= (new_size / 2);
    current -= (new_size/2);

    void *new_ptr = realloc(ptr, new_size);
    if (new_ptr) {
        total_allocated += new_size;
        current += new_size;
        if (current > peak_allocated) peak_allocated = current;
        allocation_count++;
    }
    return new_ptr;
}


void tracked_free(void *ptr, const size_t size)
{
    free(ptr);
    current -= size;
}


size_t get_alloc_count() { return allocation_count; }
size_t get_total_allocated() { return total_allocated; }
size_t get_peak_allocated() { return peak_allocated; }


void reset_memory_counter()
{
    total_allocated = 0;
    allocation_count = 0;
    peak_allocated = 0;
    current = 0;
}
