#ifndef STOGRAM_H
#define STOGRAM_H
#include <rlib.h>
#include <stdarg.h>
bool verbose = false;

char * replication_host = NULL;
unsigned int replication_port = 0;
bool replication_configured = 0;
int replication_fd = 0;
int replication_client = false;
char * server_name = NULL;

void configure_replication(char * host, unsigned int port){
    replication_host = host;
    replication_port = port;
    replication_fd = 0;
    replication_configured = 1;
    replication_client = true;
}


void sprint(char * format,...){
    if(!verbose){
        return;
    }
    va_list args;
    va_start(args, format);
    printf(format,args);
}
#endif 
