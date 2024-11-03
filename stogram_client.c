#include <rlib.h>
#include "stogram_client.h"

int main(int argc, char *argv[])
{
    server_name = strdup(uuid4());
    int port = rargs_get_option_int(argc, argv, "--port", 7001);
    char *host = "127.0.0.1";
    nsock_init(2048);
    int fd = sgc_connect(host, port);
    if(!fd){
        printf("Error connecting.\n");
        exit(1);
    }
  
    if (!sgc_subscribe(fd, "chat"))
    {
        printf("Error subscribing.\n");
        exit(0);
    }
    else
    {
        printf("Subscribed.\n");
        
    }
    for (int i = 0; i < 500; i++)
    {
        rliza_t *message = rliza_new(RLIZA_OBJECT);
        rliza_set_string(message, "writer", "tom");
        rliza_set_string(message, "reader", "user");
        rliza_set_string(message, "event", "chat");
        rliza_set_string(message, "message", "true");
        //char *json = rliza_dumps(message);
        //sgc_ping(fd,true);
        sgc_publish(fd,"publish",message);
        
        rliza_free(message);
        //sgc_ping(fd,true);
        //sgc_publish(fd, "chat", json);
        ;
        
        printf("Sent message %d\n", i);
        continue;
        char *json;
        message = sgc_read(fd);
        if (message)
        {
            json = rliza_dumps(message);
            rliza_free(message);
            printf("%s\n", json);
            free(json);
        }
    }/*
    while(true){
        rliza_t * obj = sgc_read(fd);
        if (obj)
        {
            char * json = rliza_dumps(obj);
            rliza_free(obj);
            printf("%s\n", json);
            free(json);
        }else{
            break;
        }
    }*/
    nsock_close(fd);
    return 0;
}