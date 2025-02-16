//
// Created by Seppe Degryse on 06/11/2024.
//
#include "file_source.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
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
    file_sink_t *fs = (file_sink_t*) sink;

    if (fs->sink.buffer.data)
        free(fs->sink.buffer.data);

    fs->sink.buffer = *data;
}


void push_next_fsink_write(sink_t *sink, const data_t *data)
{
    const file_sink_t *fs = (file_sink_t*) sink;

    fwrite(data->data, sizeof(triple_t), data->size*data->width, fs->file);
}


sink_t *create_file_sink(const char* path)
{
    file_sink_t *sink = malloc(sizeof(file_sink_t));
    sink->sink.buffer = (data_t) {NULL, 0, 1};

    if (path) { // There is a path so we write to a file
        sink->file = fopen(path, "wb");

        assert(sink->file);

        sink->sink.push_next = push_next_fsink_write;
    }
    else { // There is no path so we erase the data
        sink->file = NULL;
        sink->sink.push_next = push_next_fsink;
    }

    return (sink_t*) sink;
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
    file_sink_t* fsink = (file_sink_t*) sink;

    assert(fsink->sink.buffer.data);

    free(fsink->sink.buffer.data);
    fsink->sink.buffer.data = NULL;

    if (fsink->file)
        fclose(fsink->file);

    free(fsink);
    fsink = NULL;
}
