//
// Created by Seppe Degryse on 22/02/2025.
//
#include "memory.h"
#include <stdlib.h>

static size_t total_allocated = 0;
static size_t allocation_count = 0;

//static pthread_mutex_t mem_lock = PTHREAD_MUTEX_INITIALIZER;

void* tracked_malloc(size_t size) {
    //pthread_mutex_lock(&mem_lock);
    void* ptr = malloc(size);
    if (ptr) {
        total_allocated += size;
        allocation_count++;
    }
    //pthread_mutex_unlock(&mem_lock);
    return ptr;
}

size_t get_alloc_count() {return allocation_count;}
size_t get_total_allocated() {return total_allocated;}

void reset_memory_counter()
{
    total_allocated = 0;
    allocation_count = 0;
}