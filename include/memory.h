//
// Created by Seppe Degryse on 22/02/2025.
//

#ifndef MEMORY_H
#define MEMORY_H

#include <stdlib.h>

void* tracked_malloc(size_t size);

void* tracked_realloc(void* ptr, size_t new_size);

size_t get_alloc_count();

size_t get_total_allocated();

void reset_memory_counter();

#endif //MEMORY_H
