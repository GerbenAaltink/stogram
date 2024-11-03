#include <rlib.h>
#include "stogram.h"
#include "db.h"
#include "protocol.h"

void rstring_list_dump(rstring_list_t * list){
    for(int i = 0; i < list->count; i++){
        printf("%s\n", list->strings[i]);
    }
}

int main(int argc, char *argv[]){
    server_name = strdup(uuid4());
    int port = rargs_get_option_int(argc,argv,"--port",8889);
    verbose = rargs_isset(argc,argv,"--verbose");
    char * db_path = rargs_get_option_string(argc, argv, "--db", "local.db");

    char * rhost = rargs_get_option_string(argc, argv, "--rhost", NULL);
    int  rport = rargs_get_option_int(argc, argv, "--rport", 0);
        printf("Database location: %s\n",db_path);
        printf("Configured replication: %s:%d.\n",rhost,rport);
    db_connect(db_path);
    create_schema();
    
    if(rhost && rport){
        configure_replication(rhost, rport);
    }
    ensure_replication();

    //subscribe(server_name, "publish");
    //subscribe(server_name, "subscribe");
    //return 0;
    printf("Serving on port %d.\n", port);
    rprinty("Name: %s.\n", server_name);
    http_serve(port);
    db_close();
}
