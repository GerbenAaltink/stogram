#include <rlib.h>

#include "stogram.h"
#include "db.h"
#include "stogram_client.h"
#include "session.h"
#include <signal.h>
char *replication_server_name = NULL;

void on_read(int fd);
void ensure_replication()
{
    if (!replication_configured)
        return;
    if (replication_fd > 0)
        return;
    replication_configured = true;
    printf("Setting up replication to %s:%d\n", replication_host, replication_port);
    replication_fd = sgc_connect(replication_host, replication_port);
    if (!replication_fd)
    {
        rprintr("Setting up replication failed.\n");
        // exit(1);
        return;
    }
    else
    {
        printf("Setup replication: %d\n", replication_fd);
    }
    session_data_t *session = (session_data_t *)nsock_get_data(replication_fd);
    // sgc_subscribe(replication_fd, "replicate");
    //  sgc_register(replication_fd, server_name);
    // sgc_subscribe(replication_fd, "replicate");
    //  sgc_subscribe(replication_fd, "publish");
    //  sgc_subscribe(replication_fd, "publish");
    //  sgc_subscribe(replication_fd, "chat");
    // register_subscriber(session->server_name, replication_fd);
    // subscribe(session->server_name, "replicate");
    // subscribe("replicate", session->server_name);
    replication_server_name = strdup(session->server_name);
    rprinty("Replication server: %s\n", replication_server_name);
    (replication_fd, replication_server_name);
    bool subscribed = sgc_subscribe(replication_fd, "replicate");
    bool subscribed2 = sgc_subscribe(replication_fd, server_name);
    if (!subscribed || !subscribed2)
    {
        printf("ERROR!\n");
    }
    // printf("Setup replication:%d\n");
}

void on_connect(int fd)
{
    unset_subscriber_fd(fd);
    ensure_replication();
    session_data_t *data = session_new(fd);
    nsock_set_data(fd, (void *)data);

}
size_t write_object(int fd, rliza_t *obj)
{
    if (nsock_socks[fd] == 0)
    {
        return 0;
    }
    char *json = rliza_dumps(obj);
    size_t bytes_sent = nsock_write_all(fd, json, strlen(json));
    free(json);
    return bytes_sent;
}

bool write_success(int fd ,bool success){
 rliza_t *response = rliza_new(RLIZA_OBJECT);
        rliza_set_boolean(response, "result",success);
        size_t bytes_sent = write_object(fd, response);
        rliza_free(response);
    return bytes_sent > 0;
}

void broadcast(int fd, rliza_t *message)
{
    char *topic = rliza_get_string(message, "topic");
    rstring_list_t *subscribers = get_subscribers_to(topic);
    printf("Topic: %s\n", topic);
    printf("Subscribers: %d\n", subscribers->count);

    rliza_t *payload = message->get_object(message, "message");

    for (int i = 0; i < subscribers->count; i++)
    {
        int subscriber_index = 0;
        int *subscriber_fds = get_subscriber_fds_by_name(subscribers->strings[i]);

        while (subscriber_fds[subscriber_index] > 0)
        {
            printf("SPREADING EVENT TO %d\n",subscriber_fds[subscriber_index]);
            int subscriber_fd = subscriber_fds[subscriber_index];
            subscriber_index++;
            if (nsock_socks[subscriber_fd] == 0)
            {
                continue;
            }
          /*   else if (subscriber_fd == fd)
            {
                continue;
            }*/
            else if (fd == nsock_server_fd)
            {
                continue;
            }
            else if (subscriber_fd)
            {
                bool success = write_object(subscriber_fd, payload);
                if (!success)
                {
                    nsock_close(subscriber_fd);
                }
                else
                {
                    
                    // rliza_free(response);
                }
            }
        }
    }
    rstring_list_free(subscribers);
    printf("Broadcasted\n");
}

bool is_replicator(int fd)
{
    fd = nsock_socks[fd];
    if (!fd)
        return false;
    if (fd == replication_fd)
        return true;
    bool result = false;
    rstring_list_t *subscribers = get_subscribers_to("replicate");
    for (int i = 0; i < subscribers->count; i++)
    {
        int fd_replicate = get_subscriber_fd_by_name(subscribers->strings[i]);
        if (fd_replicate == fd)
        {
            result = true;
            break;
        }
    }
    return result;
}

void replicate(int sender_fd, rliza_t *message)
{
    int replications_down = 0;
    int replications_up = 0;
    if (replication_fd && replication_fd != sender_fd)
    {
        sgc_write(replication_fd, message);
        replications_up++;
    }
    rstring_list_t *subscribers = get_subscribers_to("replicate");
    for (int i = 0; i < subscribers->count; i++)
    {
        int fd_replicate = get_subscriber_fd_by_name(subscribers->strings[i]);
        if (fd_replicate == sender_fd)
        {
            continue;
        }
        sgc_write(fd_replicate, message);
        replications_down++;
    }
    rstring_list_free(subscribers);
    rprintr("Replicated %d up, %d down.\n", replications_up, replications_down);
}

ulonglong event_number = 0;
size_t handle_message(int fd, rliza_t *message)
{

    printf("START HANDLE\n");
    ensure_replication();
    session_data_t *session = (session_data_t *)nsock_get_data(fd);
    char *event = rliza_get_string(message, "event");
    if (!event){
        printf("No event. Exiting.\n");
        exit(1);
    }
    event_number++;
    printf("Event: %lld Fd: %d Name: %s\n", event_number, fd, event);
    size_t bytes_sent = 0;

    if (!strcmp(event, "ping"))
    {

        rliza_t *response = rliza_new(RLIZA_OBJECT);
         session->ping++;
        printf("%s\n",pstr("pong %lld",session->ping));
        rliza_set_string(response, "event", pstr("pong %lld",session->ping));
        bytes_sent = write_object(fd, response);
        rliza_free(response);
    }
    else if (!strcmp(event, "register"))
    {
        char *subscriber = rliza_get_string(message, "subscriber");
        if(!subscriber){
            printf("No subscriber. Existing.\n");
        }
        register_subscriber(subscriber, fd);
        rprintb("Registered: %s\n", subscriber);
        write_success(fd,true);
    }
    else if (!strcmp(event, "subscribe"))
    {
        char *subscriber = get_subscriber_by_fd(fd);
        if (!subscriber)
        {
            printf("No subscriber. Exiting.\n");
            exit(1);
        }
        char *topic = rliza_get_string(message, "topic");
        if (!topic)
        {
            printf("No topic. Exiting.\n");
            return 1;
        }
        subscribe(subscriber, topic);
        rprintb("Subscribed: %s->%s.\n", subscriber, topic);
        write_success(fd, true);    
        bytes_sent = 1;
    }
    else if (!strcmp(event, "publish"))
    {
        char *topic = rliza_get_string(message, "topic");
        printf("Publish: %s.\n", topic);
        rliza_t * event_object = rliza_get_object(message, "message");
        event_object->type = RLIZA_OBJECT;
        bytes_sent = write_object(fd, event_object);
        //write_success(fd,true);
        broadcast(fd, message);
        
    }
    else if (!strcmp(event, "execute"))
    {
        char *query = rliza_get_string(message, "query");

        rliza_t *params = rliza_get_array(message, "params");
        bytes_sent = db_execute_to_stream(fd, query, params ? params : NULL);
    }
    else if (event)
    {
        printf("Unexpected event: %s. Exiting.\n", event);
        exit(1);
    }
    else
    {
        printf("No event specified. Exiting.\n");
        exit(1);
    }
    printf("END HANDLE\n");
    return bytes_sent;
}
unsigned int read_count = 0;
void on_read(int fd)
{
    size_t bytes_sent;
    read_count++;
    int buffer_size = 1024;
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
        bool repeat = false;
        size_t length = rliza_validate(session->data);
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
            session->data_ptr = session->data;
            session->bytes_received = bytes_received;
            repeat = false;
        }
        bytes_sent = handle_message(fd, obj);
        rliza_free(obj);
        if (repeat)
        {
            on_read(fd);
        }
    }
}
void on_close(int fd)
{
    unset_subscriber_fd(fd);
    void *session = (void *)nsock_get_data(fd);
    if (session)
        free_session((session_data_t *)session);
    sprint("Connection closed. %s\n", rmalloc_stats());
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
