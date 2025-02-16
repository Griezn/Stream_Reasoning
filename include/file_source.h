//
// Created by Seppe Degryse on 06/11/2024.
//

#ifndef FILE_SOURCE_H
#define FILE_SOURCE_H
#include <stdio.h>

#include "source.h"

typedef struct FileSource {
    source_t source;
    int fd;
} file_source_t;

typedef struct FileSink {
    sink_t sink;
    FILE *file;
} file_sink_t;

source_t *create_file_source(const char *filename, uint8_t consumers);

sink_t *create_file_sink(const char* path);

void free_file_source(source_t *source);

void free_file_sink(sink_t *sink);

#endif //FILE_SOURCE_H
