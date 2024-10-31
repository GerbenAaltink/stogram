#include <rlib.h>
#include "db.h"
#include "protocol.h"

void rstring_list_dump(rstring_list_t * list){
    for(int i = 0; i < list->count; i++){
        printf("%s\n", list->strings[i]);
    }
}

int main(){

    db_connect("local.db");
    create_schema();
    //return 0;
    http_serve(8889);
    db_close();
}
