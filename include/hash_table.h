//
// Created by Seppe Degryse on 15/04/2025.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stdio.h>
#include <stdlib.h>

#include <data.h>

typedef struct Bucket {
    triple_t **tuples;
    size_t count;
    size_t capacity;
} bucket_t;

typedef struct HashTable {
    bucket_t *table;
    size_t size;
} hash_table_t;


uint32_t hash(uint32_t key, size_t table_size);

hash_table_t create_table(size_t num_elements);

void free_table(const hash_table_t *ht);

bool insert(hash_table_t *ht, uint32_t offset, triple_t *el);

bucket_t *contains(const hash_table_t *ht, uint32_t offset_in1, uint32_t offset_in2, triple_t *el);

#endif //HASH_TABLE_H
