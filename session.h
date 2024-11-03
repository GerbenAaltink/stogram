#ifndef STOGRAM_SESSION_H
#define STOGRAM_SESSION_H
#include "stogram.h"
#include <rlib.h>
#include <stdlib.h>
#include <time.h>


typedef struct session_data_t
{
    int fd;
    char * name;
    char * server_name;
    char *data;
    char *path;
    char *local_path;
    FILE *f;
    size_t size;
    char *headers;
    int content_length;
    bool received_headers;
    ulonglong ping;
    rhttp_request_t *request;
    char *data_ptr;
    unsigned int bytes_received;
} session_data_t;

session_data_t *session_new(int fd)
{
    session_data_t *session = calloc(1, sizeof(session_data_t));
    session->fd = fd;
    session->name = NULL;
    //session->server_name = server_name;
    session->data = NULL;
    session->path = NULL;
    session->local_path = NULL;
    session->f = NULL;
    session->ping = 0;
    session->size = 0;
    session->headers = NULL;
    session->content_length = 0;
    session->received_headers = false;
    session->request = NULL;
    session->data_ptr = NULL;
    session->bytes_received = 0;
    return session;
}

void free_session(session_data_t *session)
{
    if(!session)
        return;
    session->f = NULL;
    int fd = session->fd;
    session->content_length = 0;
    //if(session->name)
      //  free(session->name);
    //if(session->data)
        session->data = NULL;
    session->size = 0;
    if (session->headers)
        free(session->headers);
    session->headers = NULL;
    if (session->local_path)
        free(session->local_path);
    session->local_path = NULL;
    session->headers = NULL;
    if (session->path)
        free(session->path);
    session->path = NULL;
    free(session);
    nsock_set_data(fd, NULL);
}

#endif