#include <rlib.h>
#include "db.h"
#include <signal.h>

typedef struct session_data_t
{
    int fd;
    char *data;
    char *path;
    char *local_path;
    FILE *f;
    size_t size;
    char *headers;
    int content_length;
    bool received_headers;
    rhttp_request_t *request;
    char *data_ptr;
    unsigned int bytes_received;
} session_data_t;

void free_session(session_data_t *session)
{
    session->f = NULL;
    int fd = session->fd;
    session->content_length = 0;
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

void on_connect(int fd)
{
    // void * data = nsock_get_data(fd);
    // if(data){
    //   free(data);
    //}
    session_data_t *data = (session_data_t *)calloc(1, sizeof(session_data_t));
    data->fd = fd;
    //data->data_ptr = data->data;
    nsock_set_data(fd, (void *)data);
}
ulonglong event_number = 0;
size_t handle_message(int fd, rliza_t * message){
    char * event =rliza_get_string(message,"event");
    if(!event)
        return 0;
    event_number++;
    printf("Event: %lld Fd: %d Name: %s\n",event_number, fd, event);
    size_t bytes_sent = 0;
    if(!strcmp(event,"register")){
        char * subscriber = rliza_get_string(message,"subscriber");
        register_subscriber(subscriber,fd);
        char sql[1024] = {0};
        //sprintf(sql, "SELECT * FROM subscribers WHERE fd = %d AND time('now','-5 minutes') < last_active_time;",fd);
        strcpy(sql,"SELECT 'success'");
        bytes_sent = db_execute_to_stream(fd,sql,NULL);
    }else if(!strcmp(event,"subscribe")){
        char * subscriber = rliza_get_string(message,"subscriber");
        char * subscribed_to = rliza_get_string(message,"topic");
        int pk = subscribe(subscriber,subscribed_to);
        char sql[1024] = {0};
        sprintf(sql, "SELECT * FROM subscriptions WHERE id = %d;",fd);
        bytes_sent = db_execute_to_stream(fd,sql,NULL);
        
    }else if(!strcmp(event, "publish")){
        char * name = get_subscriber_by_fd(fd);
        if(!name){
            nsock_close(fd);
            return 0;
        }
        char * topic = rliza_get_string(message,"topic");
        char * publish_message = rliza_get_string(message,"message");
        ulonglong id =insert_publish(name,topic,publish_message);
        bytes_sent = db_execute_to_stream(fd,pstr(
            "SELECT * FROM published WHERE id = '%lld';",id   
        ),NULL);
        rstring_list_t * subscribers = get_subscribers_to(topic);
        printf("Topic: %s\n",topic);
        printf("Subscribers: %d\n",subscribers->count);
        
        for(int i = 0; i < subscribers->count; i++){
            int subscriber_fd = get_subscriber_fd_by_name(subscribers->strings[i]);
            if(nsock_socks[subscriber_fd] == 0){
                continue;
            }else if(subscriber_fd == fd){
                continue;
            }else if(subscriber_fd){
                int bytes_sent_subscriber = db_execute_to_stream(subscriber_fd,pstr(
                    "SELECT * FROM published WHERE id = '%lld';",id
                ),NULL);
                if(bytes_sent_subscriber == 0){
                    nsock_close(subscriber_fd);
                }
                bytes_sent+=bytes_sent_subscriber;
            }
        }
        rstring_list_free(subscribers);
        bytes_sent += 1;
    }else if(!strcmp(event,"execute")){
        char * query = rliza_get_string(message,"query");
        rliza_t * params = rliza_get_array(message,"params");
        bytes_sent = db_execute_to_stream(fd,query,params ? params : NULL);
    }else{
        nsock_close(fd);
        return 0;
    }
    return bytes_sent;
}

void on_read(int fd)
{
    int buffer_size = 4096;
    session_data_t *session = (session_data_t *)nsock_get_data(fd);
    int loops = 1;
    
        session->data = realloc(session->data, session->bytes_received + buffer_size + 1);
        session->data_ptr = session->data;
        int bytes_received = read(fd, session->data + session->bytes_received, buffer_size);
        if (bytes_received <= 0 || *session->data == 0 || *session->data == EOF)
        {
            nsock_close(fd);
            return;
        }
        session->bytes_received += bytes_received;
        
        session->data[session->bytes_received] = 0;
        rliza_t *obj = rliza_loads(&session->data_ptr);
        if (obj)
        {
            size_t length = session->data_ptr - session->data;
            session->bytes_received -= length;
            if (!session->bytes_received)
            {
                free(session->data);
                session->data = NULL;
                session->bytes_received = 0;
                session->data_ptr = NULL;
            }
            else
            {
                char *new_data = (char *)malloc(session->bytes_received + 1);
                strncpy(new_data, session->data + length, session->bytes_received);
                free(session->data);
                session->data = new_data;
                session->data_ptr = new_data;
            }
            handle_message(fd, obj);
            rliza_free(obj);
        
    }
}
void on_close(int fd)
{
    unset_subscriber_fd(fd);
    void *session = (void *)nsock_get_data(fd);
    if(session)
        free_session((session_data_t *)session);
    printf("%s\n", rmalloc_stats());
    // void * data = nsock_get_data(fd);
    // if(data)
    //   free(data);
    return;
}

void http_serve(int port)
{
    signal(SIGPIPE, SIG_IGN);

    nsock(port, on_connect, on_read, on_close);
}

char *http_read_request(int fd)
{
    char buffer[12000];
    char *buffer_ptr = buffer;
    char c[2];
    while (true)
    {
        int bytes_received = read(fd, c, 1);
        if (bytes_received <= 0 || *c == 0 || *c == EOF)
        {
            nsock_close(fd);
            return NULL;
        }
        *buffer_ptr = *c;
        buffer_ptr++;
        *buffer_ptr = 0;
        if (rstrendswith(buffer, "\r\n\r\n"))
        {
            break;
        }
    }
    return sbuf(buffer);
}