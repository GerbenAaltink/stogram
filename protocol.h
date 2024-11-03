#include <rlib.h>

#include "stogram.h"
#include "db.h"
#include "stogram_client.h"
#include "session.h"
#include <signal.h>
char * replication_server_name = NULL;

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
    //sgc_subscribe(replication_fd, "replicate");
    // sgc_register(replication_fd, server_name);
    //sgc_subscribe(replication_fd, "replicate");
    // sgc_subscribe(replication_fd, "publish");
    // sgc_subscribe(replication_fd, "publish");
    // sgc_subscribe(replication_fd, "chat");
    //register_subscriber(session->server_name, replication_fd);
    //subscribe(session->server_name, "replicate");
    //subscribe("replicate", session->server_name);
    replication_server_name = strdup(session->server_name);
    rprinty("Replication server: %s\n", replication_server_name);
    (replication_fd, replication_server_name);
    bool subscribed = sgc_subscribe(replication_fd, "replicate");
    bool subscribed2 = sgc_subscribe(replication_fd, server_name);
    if(!subscribed || !subscribed2){
        printf("ERROR!\n");
    }
    // printf("Setup replication:%d\n");
}

void on_connect(int fd)
{

    printf("HERE 24\n");
    ensure_replication();
    session_data_t *data = session_new(fd);
    nsock_set_data(fd, (void *)data);

    printf("HERE 25\n");
}
size_t write_object(int fd, rliza_t *obj)
{

    printf("HERE 26\n");
    if(nsock_socks[fd] == 0){
        printf("ERROR!\n");
        return 0;
    }
    char *json = rliza_dumps(obj);
    size_t bytes_sent = nsock_write_all(fd, json, strlen(json));
   free(json);

    printf("HERE 27\n");
    return bytes_sent;
}
size_t broadcast(int fd, rliza_t *message)
{
    char *topic = rliza_get_string(message, "topic");
    rstring_list_t *subscribers = get_subscribers_to(topic);
    printf("Topic: %s\n", topic);
    printf("Subscribers: %d\n", subscribers->count);
    
    size_t bytes_sent = 0;
    rliza_t * payload = rliza_get_object(message, "message");
    free(payload);
    for (int i = 0; i < subscribers->count; i++)
    {
        int subscriber_index = 0;
        int * subscriber_fds = get_subscriber_fds_by_name(subscribers->strings[i]);
        
        while(subscriber_fds[subscriber_index] > 0){
            int subscriber_fd = subscriber_fds[subscriber_index];
        if (nsock_socks[subscriber_fd] == 0)
        {
            continue;
        }
        else if (subscriber_fd == fd)
        {
            continue;
        }
        else if (subscriber_fd)
        {
            int bytes_sent_subscriber = write_object(subscriber_fd, payload);

            if (bytes_sent_subscriber == 0)
            {
                nsock_close(subscriber_fd);
            }
            bytes_sent += bytes_sent_subscriber;
        }
        subscriber_index++;
        }
    }
    rstring_list_free(subscribers);
    return bytes_sent;
}

bool replicate2(int fd, rliza_t *message)
{
    if (!replication_configured)
        return false;
    if (replication_client == false)
    {
        broadcast(fd, message);
        return true;
    }
    rliza_t *replicate_message = rliza_new(RLIZA_OBJECT);
    replicate_message->set_object(replicate_message, "message", message);

    replicate_message->set_string(replicate_message, "event", "replicate");
    sgc_write(replication_fd, replicate_message);
    // HIER MEMORY MESSAGE MOET WEG;
    // broadcast(fd, message, "replicate");
    rliza_free(replicate_message);
    printf("Replicatedd!!\n");

    return false;
}

bool is_replicator(int fd){
    fd = nsock_socks[fd];
    if(!fd)
        return false;
    if(fd == replication_fd) return true;
    bool result = false;
    rstring_list_t * subscribers = get_subscribers_to("replicate");
    for (int i = 0; i < subscribers->count; i++)
    {
        int fd_replicate = get_subscriber_fd_by_name(subscribers->strings[i]);
        if(fd_replicate == fd){
            result = true ;
            break;
        }
    
    }
    return result;
}

void replicate(int sender_fd,rliza_t * message){
    int replications_down = 0;
    int replications_up = 0;
    if(replication_fd && replication_fd != sender_fd){
        sgc_call(replication_fd, message);
        replications_up++;
    }
    rstring_list_t * subscribers = get_subscribers_to("replicate");
    for (int i = 0; i < subscribers->count; i++)
    {
        int fd_replicate = get_subscriber_fd_by_name(subscribers->strings[i]);
        if(fd_replicate == sender_fd){
            continue;
        }
        sgc_call(fd_replicate,message);
            replications_down++;

    
    }
    rstring_list_free(subscribers);
    rprintr("Replicated %d up, %d down.\n",replications_up,replications_down);
}

ulonglong event_number = 0;
size_t handle_message(int fd, rliza_t *message)
{

    printf("HERE 30\n");
    bool is_replicating = is_replicator(fd);
    ensure_replication();
    replicate(fd,message);
    // replicate(fd,message);
    session_data_t *session = (session_data_t *)nsock_get_data(fd);
    bool do_replicate = fd != replication_fd && replication_client == true;

    printf("HERE 31\n");
    char *event = rliza_get_string(message, "event");
    if (!event)
        return 0;
    event_number++;
    printf("Event: %lld Fd: %d Name: %s\n", event_number, fd, event);
    size_t bytes_sent = 0;
    
    if (!strcmp(event, "ping"))
    {
        
        rliza_t *response = rliza_new(RLIZA_OBJECT);
        rliza_set_string(response, "event", pstr("pong %lld",session->ping++));
        bytes_sent = write_object(fd, response);

        //replicate(fd, message);
        rliza_free(response);
        
        bytes_sent = 1;
    } else 
    if (!strcmp(event, "register"))
    {
        char *subscriber = rliza_get_string(message, "subscriber");
        if(!subscriber){
            rprintr("GEEN SUBSCRIBER!!\n");
            return 1;
        }
        register_subscriber(subscriber, fd);
        //sgc_subscribe(fd, subscriber);
        //register_subscriber(subscriber, replication_fd);
        rprintb("Registered: %s\n",subscriber);
        
        rliza_t *response = rliza_new(RLIZA_OBJECT);
        rliza_set_string(response, "server_name", server_name);
        bytes_sent = write_object(fd, response);

        //replicate(fd, message);
        rliza_free(response);
    }
    else if (!strcmp(event, "subscribe"))
    {
        char * subscriber = get_subscriber_by_fd(fd);
        if(!subscriber){
            return 1;
        }
        //char *subscriber = rliza_get_string(message, "subscriber");
        char *subscribed_to = rliza_get_string(message, "topic");
        if(!subscribed_to){
            return 1;
        }
        
        subscribe(subscriber, subscribed_to);
        register_subscriber(subscriber, fd);
            
        rprintb("Subscribed: %s->%s.\n", subscriber,subscribed_to);
        if(!strcmp(subscribed_to,"replicate")){
            rprintg("Registered client subsriber");
            //return 1;
        }
        rliza_t *response = rliza_new(RLIZA_OBJECT);
        rliza_set_boolean(response, "success", true);
        rliza_set_string(response, "message", "Successfully subscribed!!");
        bytes_sent = write_object(fd, response);
        rliza_free(response);
       // write_object(fd,message);
        //if(replication_server_name)
        //register_subscriber(subscriber, replication_fd);
       // broadcast(fd, message);
        //if(replication_fd)
          //  write_object(replication_fd, message);
        bytes_sent = 1;
    }
    else if (!strcmp(event, "replicates"))
    {
        printf("print repl\n");
        //  rliza_t * obj = rliza_get_object(message,"message");
        char *json = rliza_dumps(message);

        printf("RECV:%s\n", json);
        //  printf("Received replication message: %s\n",json);
        
        broadcast(fd, message);
        printf("WEEE\n");
        bytes_sent = 1;
    }
    else if (!strcmp(event, "publish"))
    {
        char *topic = rliza_get_string(message, "topic");
        printf("Publish: %s.\n",topic);
         rliza_t *response = rliza_new(RLIZA_OBJECT);
        rliza_set_string(response, "server_name", server_name);
        bytes_sent = write_object(fd, response);
        rliza_free(response);
        //write_object(fd, message);
        broadcast(fd, message);
        bytes_sent = 1;
    }
    else if (!strcmp(event, "execute"))
    {
        char *query = rliza_get_string(message, "query");
        rliza_t *params = rliza_get_array(message, "params");
        bytes_sent = db_execute_to_stream(fd, query, params ? params : NULL);
    }
    else if (event)
    {
      //  replicate(message);
    }
    else
    {
        printf("HIER!!!\n");
        nsock_close(fd);
        return 0;
    }
    if(rfd_wait(fd, 0)){
        on_read(fd);
    }
    return bytes_sent;
}
unsigned int read_count = 0;
void on_read(int fd)
{

    printf("HERE 40\n");
    size_t bytes_sent;
    read_count++;
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
     bool repeat = false;
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
            repeat = false;
        }

        printf("HERE 53\n");
        bytes_sent = handle_message(fd, obj);

    printf("HERE 54\n");
        rliza_free(obj);
        if(repeat){
            printf("REPEAT: %d",repeat);
           // on_read(fd);
        }
    
    }

    printf("HERE 50\n");
    
}
void on_close(int fd)
{
    unset_subscriber_fd(fd);
    void *session = (void *)nsock_get_data(fd);
    if (session)
        free_session((session_data_t *)session);
    sprint("Connection closed. %s\n", rmalloc_stats());
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
