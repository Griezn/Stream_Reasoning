//
// Created by Seppe Degryse on 15/04/2025.
//
#include "hash_table.h"

#include <assert.h>


uint32_t hash(const uint32_t key, const size_t table_size)
{
    return key % table_size;
}


hash_table_t create_table(const size_t num_elements)
{
    const size_t size = num_elements * 2;  // double for lower load factor
    hash_table_t ht;
    ht.size = size;

    // Allocate array of buckets
    ht.table = (bucket_t *) calloc(size, sizeof(bucket_t));
    if (ht.table == NULL) {
        fprintf(stderr, "Failed to allocate hash table\n");
        exit(EXIT_FAILURE);
    }

    return ht;
}


void free_table(const hash_table_t *ht)
{
    for (size_t i = 0; i < ht->size; i++) {
        free(ht->table[i].tuples);
    }
    free(ht->table);
}


bool bucket_insert(bucket_t *bucket, triple_t *el) {
    if (bucket->count == bucket->capacity) {
        size_t new_capacity = bucket->capacity == 0 ? 4 : bucket->capacity * 2;
        triple_t **new_tuples = realloc(bucket->tuples, new_capacity * sizeof(triple_t *));
        if (new_tuples == NULL) return false;

        bucket->tuples = new_tuples;
        bucket->capacity = new_capacity;
    }

    bucket->tuples[bucket->count++] = el;
    return true;
}


bool triple_offset_equal(const uint32_t offset, triple_t *el, uint32_t key)
{
    return *((uint32_t *) el + offset) == key;
}


bool insert(hash_table_t *ht, const uint32_t offset, triple_t *el) {
    uint32_t key = *((uint32_t *)el + offset);
    size_t idx = hash(key, ht->size);

    for (size_t i = 0; i < ht->size; i++) {
        size_t probe_idx = (idx + i) % ht->size;
        bucket_t *bucket = &ht->table[probe_idx];

        // Empty bucket: initialize it
        if (bucket->tuples == NULL) {
            bucket->tuples = malloc(4 * sizeof(triple_t *));
            if (bucket->tuples == NULL) return false;
            bucket->capacity = 4;
            bucket->count = 0;
        }

        // Check if the key in this bucket matches or it's the first insert
        if (bucket->count == 0 || triple_offset_equal(offset, bucket->tuples[0], key)) {
            return bucket_insert(bucket, el);
        }
    }

    return false; // Table full
}


bucket_t *contains(const hash_table_t *ht, const uint32_t offset_in1, const uint32_t offset_in2, triple_t *el) {
    uint32_t key = *((uint32_t *)el + offset_in2);
    size_t idx = hash(key, ht->size);

    for (size_t i = 0; i < ht->size; i++) {
        size_t probe_idx = (idx + i) % ht->size;
        bucket_t *bucket = &ht->table[probe_idx];

        if (bucket->tuples == NULL)
            return NULL;

        if (bucket->count > 0 && triple_offset_equal(offset_in1, bucket->tuples[0], key))
            return bucket;
    }

    return NULL;
}
