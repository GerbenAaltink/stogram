#include <rlib.h>

#include "stogram.h"
#include "db.h"
#include "stogram_client.h"
#include "session.h"
#include <signal.h>

void ensure_replication(){
    if(!replication_configured)
        return;
    if(replication_fd > 0)
        return;
    printf("Setting up replication to %s:%d\n",replication_host,replication_port);
    replication_fd = sgc_connect(replication_host, replication_port);
    if(!replication_fd){
        printf("Setting up replication failed.\n");
        exit(1);
    }else{
        printf("Setup replication: %d\n", replication_fd);
    }
    session_data_t * session = (session_data_t *)nsock_get_data(replication_fd);
    //sgc_register(replication_fd, server_name);
    sgc_subscribe(replication_fd, "replicate");
    //sgc_subscribe(replication_fd, "publish");
    //sgc_subscribe(replication_fd, "publish");
    //sgc_subscribe(replication_fd, "chat");
    register_subscriber(session->server_name,replication_fd);
    subscribe(session->server_name, "replicate");
    subscribe("replicate",session->server_name);
    printf("Replication server: %s\n",session->server_name);
    bool subscribed = sgc_subscribe(replication_fd, "publish");
    //printf("Setup replication:%d\n");
}



void on_connect(int fd)
{
    ensure_replication();
    session_data_t *data = session_new(fd);
    nsock_set_data(fd, (void *)data);
}
size_t write_object(int fd, rliza_t * obj){
    char * json = rliza_dumps(obj);
    size_t bytes_sent = nsock_write_all(fd, json, strlen(json) );
    free(json);
    return bytes_sent;
}
size_t broadcast(int fd, rliza_t * message, char * topic){
     rstring_list_t * subscribers = get_subscribers_to(topic);
        printf("Topic: %s\n",topic);
        printf("Subscribers: %d\n",subscribers->count);
        size_t bytes_sent = 0;
        
        for(int i = 0; i < subscribers->count; i++){
            int subscriber_fd = get_subscriber_fd_by_name(subscribers->strings[i]);
            if(nsock_socks[subscriber_fd] == 0){
                continue;
            }else if(subscriber_fd == fd){
                continue;
            }else if(subscriber_fd){
                int bytes_sent_subscriber = write_object(subscriber_fd, message);

                if(bytes_sent_subscriber == 0){
                    nsock_close(subscriber_fd);
                }
                bytes_sent+=bytes_sent_subscriber;
            }
        }
        rstring_list_free(subscribers);
        return bytes_sent;
}

bool replicate(int fd, rliza_t * message){
    char * event = message->get_string(message,"event");
    if(event && strcmp(event,"replicate")){
        broadcast(fd, message, "replicate");
        
    }
    return false;
}

ulonglong event_number = 0;
size_t handle_message(int fd, rliza_t * message){
    ensure_replication();
   // replicate(fd,message);
    session_data_t * session = (session_data_t *)nsock_get_data(fd);
    bool do_replicate = fd != replication_fd && replication_client == true;
    
    char * event =rliza_get_string(message,"event");
    if(!event)
        return 0;
    event_number++;
    printf("Event: %lld Fd: %d Name: %s\n",event_number, fd, event);
    size_t bytes_sent = 0;
    if(!strcmp(event,"register")){
        char * subscriber = rliza_get_string(message,"subscriber");
        register_subscriber(subscriber,fd);
        printf("Registered subscriber\n");
        char sql[1024] = {0};
        
        rliza_t * response = rliza_new(RLIZA_OBJECT);
        rliza_set_string(response, "server_name",server_name);
        bytes_sent = write_object(fd, response);

        //replicate(fd,message);
        rliza_free(response);
    }else if(!strcmp(event,"subscribe")){
        char * subscriber = rliza_get_string(message,"subscriber");
        char * subscribed_to = rliza_get_string(message,"topic");
        subscribe(subscriber,subscribed_to);
        bytes_sent = 1;
    }else if(!strcmp(event,"replicate")) {
         char * subscriber = rliza_get_string(message,"subscriber");
        char * subscribed_to = "replicate";
        int pk = subscribe(subscriber,subscribed_to);
        //char sql[1024] = {0};
        //sprintf(sql, "SELECT * FROM subscriptions  WHERE id = %d;",fd);
        //bytes_sent = db_execute_to_stream(fd,sql,NULL);
          
        }else if(!strcmp(event, "publish")){

         
        char * topic = rliza_get_string(message,"topic");
        char * publish_message = rliza_get_string(message,"message");
        
        //if(publish_message != NULL){
        if(strcmp(topic,"replicate")){
            replicate(fd,message);
            printf("%s\n",publish_message);
            broadcast(fd,message,topic);
           
        }
        //}
        
        
        bytes_sent = 1;
    }else if(!strcmp(event,"execute")){
        char * query = rliza_get_string(message,"query");
        rliza_t * params = rliza_get_array(message,"params");
        bytes_sent = db_execute_to_stream(fd,query,params ? params : NULL);
    }else if(event){
        replicate(fd,message);    
    }else{
        printf("HIER!!!\n");
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
