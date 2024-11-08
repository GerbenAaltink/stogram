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
    for (int i = 0; i < 10000; i++)
    {
        rliza_t *message = rliza_new(RLIZA_OBJECT);
        rliza_set_string(message, "writer", "tom");
        rliza_set_string(message, "reader", "user");
        rliza_set_string(message, "event", "chat");
        rliza_set_string(message, "message", "true");
        sgc_publish(fd,"chat",message);
        rliza_free(message);
        printf("Sent message %d\n", i);
    }
    nsock_close(fd);
    return 0;
}
