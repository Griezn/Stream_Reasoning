//
// Created by Seppe Degryse on 06/11/2024.
//

#ifndef FILE_SOURCE_H
#define FILE_SOURCE_H
#include "source.h"

typedef struct FileSource {
    source_t source;
    int fd;
    uint32_t index;
    uint32_t inc;
} file_source_t;

source_t *create_file_source(const char *filename, uint8_t wsize, uint32_t wstep);

sink_t *create_file_sink();

void free_file_source(source_t *source);

void free_file_sink(sink_t *sink);

#endif //FILE_SOURCE_H
