//
// Created by Seppe Degryse on 01/12/2024.
//
#include "mutli_source.h"

#include <stdlib.h>
#include <stdio.h>


data_t *get_next_multi_source(const source_t *source, const uint32_t size, const uint32_t step)
{
    multi_source_t *ms = (multi_source_t*) source;
    data_t* datas[ms->num_sources];
    uint32_t sizes[ms->num_sources];
    data_t* out = malloc(sizeof(data_t));
    out->size = 0;
    out->width = 1;

    // fetch data, set size
    for (int i = 0; i < ms->num_sources; ++i) {
        data_t *next = ms->sources[i]->get_next(ms->sources[i], size, step);

        if (next == NULL)
            return NULL;

        datas[i] = next;
        sizes[i] = 0;
        out->size += datas[i]->size;
    }

    // allocate the output array
    out->data = malloc(sizeof(triple_t) * out->size);

    uint32_t i = 0;
    while (i < out->size) { // loop until all elements are processed
        for (int j = 0; j < ms->num_sources; ++j) {
            if (sizes[j] < datas[j]->size) { // check if an array is fully processed
                out->data[i++] = datas[j]->data[sizes[j]++];
            }
        }
    }
    
    return out;
}


source_t *create_multi_source()
{
    multi_source_t *ms = malloc(sizeof(multi_source_t));
    ms->num_sources = 0;
    ms->capacity = 2;
    ms->sources = malloc(ms->capacity * sizeof(source_t*));
    ms->source.get_next = get_next_multi_source;

    return (source_t*) ms;
}


void push_next_msink(sink_t *sink, const data_t *data)
{
    if (sink->buffer.data)
        free(sink->buffer.data);

    sink->buffer = *data;
}


sink_t *create_multi_sink()
{
    sink_t *sink = malloc(sizeof(sink_t));
    sink->buffer = (data_t) {NULL, 0, 1};
    sink->push_next = push_next_msink;
    return sink;
}


void multi_source_add(source_t* multi_source, const source_t* source)
{
    multi_source_t *ms = (multi_source_t*) multi_source;
    if (ms->num_sources == ms->capacity) {
        ms->capacity = ms->capacity * 2;
        ms->sources = realloc(ms->sources, ms->capacity);
    }

    ms->sources[ms->num_sources++] = (source_t*) source;
}


void free_multi_source(source_t *source)
{
    multi_source_t *ms = (multi_source_t*) source;
    free(ms->sources);
    ms->sources = NULL;
    free(ms);
    ms = NULL;
}


void free_multi_sink(sink_t *sink)
{
    free(sink);
    sink = NULL;
}
