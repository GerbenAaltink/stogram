#ifndef STOGRAM_CLIENT_H
#define STOGRAM_CLIENT_H
#include <rlib.h>
#include <stdlib.h>
#include <time.h>
#include "session.h"

rliza_t *sgc_read(int fd);

size_t sgc_write(int fd, rliza_t *message)
{
    char *json = rliza_dumps(message);
    //printf("%s\n",json);
    size_t bytes_sent = nsock_write_all(fd, json, strlen(json) );
    if (bytes_sent == 0)
    {
        free(json);
        nsock_close(fd);
        return false;
    }
    free(json);
    return bytes_sent;
}
rliza_t *sgc_call(int fd, rliza_t *message)
{

    if (!sgc_write(fd, message))
        return NULL;
    return sgc_read(fd);
}

int sgc_connect(char *host, int port)
{
    int fd = nsock_connect(host, port);
    if (fd == 0)
        return 0;

    session_data_t *session = session_new(fd);
    session->name = strdup(server_name);
    nsock_set_data(fd, (void *)session);
    char *subscriber = server_name;
    rliza_t *message = rliza_new(RLIZA_OBJECT);
    rliza_set_string(message, "event", "register");
    rliza_set_string(message, "subscriber", server_name);

    rliza_t * result = sgc_call(fd, message); // nsock_write_all(fd, json, rliza_validate(json));
        rliza_free(message);

    if (!result)
    {

        nsock_close(fd);
        fd = 0;
        return fd;
    }
       rliza_free(result);
    
    //session->server_name = strdup(uuid4()); //strdup(result->get_string(result, "server_name"));
    
    return fd;
}

rliza_t *sgc_read(int fd)
{
    session_data_t *session = (session_data_t *)nsock_get_data(fd);
    size_t buffer_size = 100;
    rliza_t *obj = NULL;
    while (true)
    {
        session->data = realloc(session->data, session->bytes_received + buffer_size + 1);
        session->data_ptr = session->data;

        int bytes_received = read(fd, session->data + session->bytes_received, buffer_size);

        if (bytes_received <= 0 || *session->data == 0 || *session->data == EOF)
        {
            printf("CLOSE!!\n");
            free_session(session);
            nsock_close(fd);
            fd = 0;
            return NULL;
        }

        session->bytes_received += bytes_received;

        session->data[session->bytes_received] = 0;
        rliza_t *obj = rliza_loads(&session->data_ptr);
         if (obj)
    {
        //free(session->data);
        //session->data = NULL;
       // session->bytes_received = 0;
       // session->bytes_received = 0;
       // return obj;
        bool repeat = false;
        size_t length = rliza_validate(session->data);
        session->bytes_received -= length;
        if (!session->bytes_received)
        {
            return obj;
            free(session->data);
            session->data = NULL;
            session->bytes_received = 0;
            session->data_ptr = NULL;
            
        }
        else
        {
            printf("GRRR\n");
            char *new_data = (char *)malloc(session->bytes_received + 1);
            strncpy(new_data, session->data + length, session->bytes_received);
            free(session->data);
            session->data = new_data;
            session->data_ptr = session->data;
            session->bytes_received = session->bytes_received;
            repeat = false;
        }
        return obj;
    }
    }
    return NULL;
}

size_t sgc_subscribe(int fd, char *topic)
{
    rliza_t *message = rliza_new(RLIZA_OBJECT);
    rliza_set_string(message, "event", "subscribe");
    rliza_set_string(message, "subscriber", uuid4());
    rliza_set_string(message, "topic", topic);
    rliza_t * result = sgc_call(fd, message);
    if(result){
        rliza_free(result);
        return true;
    }
    return false;
}

bool sgc_publish(int fd, char *topic, rliza_t *message)
{   
    rliza_t *payload = rliza_new(RLIZA_OBJECT);
    payload->set_string(payload, "event", "publish");
    payload->set_string(payload, "topic", topic);
    payload->set_object(payload, "message",message);
    //payload->set_object(payload, "message", message);
    char * json = rliza_dumps(payload);
    printf("%s\n", json);
    rliza_t * result = sgc_call(fd, payload);
    rliza_free(payload);
    if(result){
        rliza_free(result);;
        return true;
    }
    return false;
}

bool sgc_ping(int fd, bool prin){
rliza_t *message = rliza_new(RLIZA_OBJECT);
        rliza_set_string(message, "writer", "tom");
        rliza_set_string(message, "reader", "user");
        rliza_set_string(message, "event", "ping");
        rliza_set_string(message, "message", "ping");
        rliza_t * response = sgc_call(fd, message);
        if(prin){
        char *json = rliza_dumps(response);
           
            free(json);
        }
        rliza_free(response);
        rliza_free(message);
         return true;
         }

#endif
