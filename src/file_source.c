//
// Created by Seppe Degryse on 06/11/2024.
//
#include "file_source.h"

#include <assert.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "generator.h"
#include "utils.h"


data_t *get_next_file(const source_t *source, const uint32_t size, const uint32_t step)
{
    file_source_t *fs = (file_source_t*) source;
    if (fs->source.index + step > fs->source.buffer.size) {
        return NULL;
    }

    data_t *data = malloc(sizeof(data_t));
    assert(data);
    data->data = fs->source.buffer.data + (fs->source.index * fs->source.buffer.width);
    data->size = min(size, fs->source.buffer.size - fs->source.index);
    data->width = source->buffer.width;

    if (++fs->source.consumed == fs->source.consumers) {
        fs->source.index += step;
        fs->source.consumed = 0;
    }

    return data;
}


source_t *create_file_source(const char *filename, const uint8_t consumers)
{
    file_source_t *fs = malloc(sizeof(file_source_t));

    fs->fd = open(filename, O_RDONLY);
    if (fs->fd == -1) {
        free(fs);
        return NULL;
    }

    struct stat sb;
    if (fstat(fs->fd, &sb) == -1) {
        close(fs->fd);
        free(fs);
        return NULL;
    }

    struct Triple *triples = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fs->fd, 0);
    if (triples == MAP_FAILED) {
        close(fs->fd);
        free(fs);
        return NULL;
    }
    fs->source.buffer.data = triples;
    fs->source.buffer.size = sb.st_size / sizeof(triple_t);
    fs->source.buffer.width = 1;
    fs->source.get_next = get_next_file;
    fs->source.index = 0;
    fs->source.consumers = consumers;
    fs->source.consumed = 0;

    return  (source_t*) fs;
}

void push_next_fsink(sink_t *sink, const data_t *data)
{
    if (sink->buffer.data)
        free(sink->buffer.data);

    sink->buffer = *data;
}


sink_t *create_file_sink()
{
    sink_t *sink = malloc(sizeof(sink_t));
    sink->buffer = (data_t) {NULL, 0, 1};
    sink->push_next = push_next_fsink;
    return sink;
}

void free_file_source(source_t *source)
{
    file_source_t *fs = (file_source_t*) source;
    munmap(fs->source.buffer.data, fs->source.buffer.size * sizeof(triple_t));
    close(fs->fd);

    free(fs);
    fs = NULL;
}

void free_file_sink(sink_t *sink)
{
    assert(sink->buffer.data);

    free(sink->buffer.data);
    sink->buffer.data = NULL;

    free(sink);
    sink = NULL;
}
