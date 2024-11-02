#ifndef STOGRAM_DB_H
#define STOGRAM_DB_H
#include <rlib.h>
#include "stogram.h"
#include <sqlite3.h>
ulonglong _ruid = 0;



sqlite3 * db; 



void raise_db_error(){
    printf("Fatal error occured: %s.\nExiting application.\n",sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
}

void db_connect(char * path){
    if(sqlite3_open(path,&db) != SQLITE_OK){
        raise_db_error();
    }
    sqlite3_exec(db, "PRAGMA synchronous = OFF;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA journal_mode = MEMORY;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA cache_size = -2000;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA temp_store = MEMORY;", NULL, NULL, NULL);
}
void db_close(){
    if(sqlite3_close(db) != SQLITE_OK){
        raise_db_error();
    }
}

size_t db_execute_to_stream(int fd, char *query, rliza_t *params) {
    sqlite3_stmt *stmt = 0;
    char * escaped_query = (char *)malloc(strlen(query) + 1);
    
    rstrstripslashes(query,escaped_query);
    
    if (sqlite3_prepare_v2(db, escaped_query, -1, &stmt, 0) != SQLITE_OK) {
        free(escaped_query);

        rliza_t *error = rliza_new(RLIZA_OBJECT);
        error->set_string(error, "error", (char *)sqlite3_errmsg(db));
        error->set_boolean(error, "success", false);
        char *json_response = (char *)rliza_dumps(error);

        size_t bytes_sent = nsock_write_all(fd,(unsigned char *)json_response,strlen(json_response)); //write_http_chunk(request, json_response, true);
        free(json_response);
        rliza_free(error);

        return bytes_sent;
    } else {
        
        free(escaped_query);

        if(params){
            
            for (unsigned col = 0; col < params->count; col++) {
                if (params->content.map[col]->type == RLIZA_INTEGER) {
                    sqlite3_bind_int(stmt, col + 1, params->content.map[col]->content.integer);
                } else if (params->content.map[col]->type == RLIZA_STRING) {
                    if (params->content.map[col]->content.string && *params->content.map[col]->content.string) {
                        sqlite3_bind_text(stmt, col + 1, (char *)params->content.map[col]->content.string, -1, SQLITE_STATIC);
                    } else {
                        sqlite3_bind_null(stmt, col + 1);
                    }
                } else if (params->content.map[col]->type == RLIZA_BOOLEAN) {
                    sqlite3_bind_int(stmt, col + 1, params->content.map[col]->content.boolean);
                } else if (params->content.map[col]->type == RLIZA_NULL) {
                    sqlite3_bind_null(stmt, col + 1);
                } else if (params->content.map[col]->type == RLIZA_NUMBER) {
                    sqlite3_bind_double(stmt, col + 1, params->content.map[col]->content.number);
                } else {
                    printf("Unknown parameter type: %s\n", params->content.map[col]->key);
                }
            }
        }
       
        rliza_t *result = rliza_new(RLIZA_OBJECT);
        result->set_boolean(result, "success", true);
       
        if(!sqlite3_strnicmp(query, "UPDATE", 6) || !sqlite3_strnicmp(query, "DELETE", 6) || !sqlite3_strnicmp(query, "DROP", 6)) {

            int rows_affected = sqlite3_changes(db);
            result->set_integer(result, "rows_affected", rows_affected);
        }else if(!sqlite3_strnicmp(query, "INSERT", 6)) {
            sqlite3_int64 last_insert_id = sqlite3_last_insert_rowid(db);
            if(last_insert_id)
                result->set_integer(result, "last_insert_id",last_insert_id);
        

        }
        char *json_response = (char *)rliza_dumps(result);
        json_response[strlen(json_response) - 1] = '\0';
        int bytes_sent = nsock_write_all(fd,json_response,strlen(json_response)); 
        free(json_response);
        if (!bytes_sent){
            nsock_close(fd);
            return 0;
        }
        long long count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            rliza_t *row = rliza_new(RLIZA_ARRAY);
            if (count == 0) {
                json_response = (char *)malloc(1024 * 1024);
                json_response[0] = 0;
                strcpy(json_response, ",\"columns\":[");
                for (int col = 0; col < sqlite3_column_count(stmt); col++) {
                    char * column_name = (char *)sqlite3_column_name(stmt, col);
                    char * escaped_column_name = (char *)malloc(strlen(column_name) * 2 + 1);
                    rstraddslashes(column_name,escaped_column_name); 

                    strcat(json_response, "\"");
                    strcat(json_response, escaped_column_name);
                    free(escaped_column_name);
                    strcat(json_response, "\",");
                }
                if (json_response[strlen(json_response) - 1] == ',')
                    json_response[strlen(json_response) - 1] = ']';
                strcat(json_response, ",\"rows\":[");
                bytes_sent = nsock_write_all(fd,(unsigned char *)json_response,strlen(json_response));
                free(json_response);
                rliza_free(result);
                if(bytes_sent == 0){
                    nsock_close(fd);
                    return 0;
                }
            }

            for (int col = 0; col < sqlite3_column_count(stmt); col++) {
                int type = sqlite3_column_type(stmt, col);
                switch (type) {
                case SQLITE_INTEGER:
                    rliza_push(row, rliza_new_integer((long long)sqlite3_column_int(stmt, col)));
                    break;

                case SQLITE_FLOAT:
                    rliza_push(row, rliza_new_number(sqlite3_column_double(stmt, col)));
                    break;
                case SQLITE_TEXT:
                    char *string = (char *)sqlite3_column_text(stmt, col);
                    if (string && !strcmp(string, "true")) {
                        rliza_push(row, rliza_new_boolean(true));
                    } else if (string && !strcmp(string, "false")) {
                        rliza_push(row, rliza_new_boolean(false));
                    } else {
                        rliza_push(row, rliza_new_string(string));
                    }
                    break;
                case SQLITE_BLOB:
                    break;
                case SQLITE_NULL:
                    rliza_push(row, rliza_new_null());
                    break;
                default:
                    printf("Unknown column type\n");
                }
            }
            count++;
            json_response = (char *)rliza_dumps(row);
            char *prefixed_json_response = (char *)malloc(strlen(json_response) + 1024);
            prefixed_json_response[0] = 0;
            if (count > 1)
                strcpy(prefixed_json_response, ",");
            strcat(prefixed_json_response, json_response);
            free(json_response);
            size_t bytes_sent_chunk = nsock_write_all(fd, (unsigned char*)prefixed_json_response, strlen(prefixed_json_response));
            bytes_sent += bytes_sent_chunk;
            free(prefixed_json_response);
            rliza_free(row);
            if (!bytes_sent_chunk)
                return 0;
        }
        if(count == 0)
            rliza_free(result);
        sqlite3_finalize(stmt);
        char *rows_end = strdup(count ? "]" : "");
        char *footer_end = (char *)malloc(512);
        sprintf(footer_end, ",\"count\":%lld}", count);
        json_response = (char *)malloc(1024);
        sprintf(json_response, "%s,%s\r\n", rows_end, footer_end + 1);
        free(rows_end);
        free(footer_end);
        size_t bytes_sent_chunk = nsock_write_all(fd, (unsigned char *)json_response, strlen(json_response));
        bytes_sent += bytes_sent_chunk;
        free(json_response);
        if (bytes_sent_chunk == 0)
            return 0;
        return bytes_sent;
    }
    return 0;
}

size_t db_execute_to_stream2(int fd, char *query, rliza_t *params) {
    sqlite3_stmt *stmt = 0;
    char * escaped_query = (char *)malloc(strlen(query) + 1);
    rstrstripslashes(query,escaped_query);
    if (sqlite3_prepare_v2(db, escaped_query, -1, &stmt, 0) != SQLITE_OK) {
        free(escaped_query);
        rliza_t *error = rliza_new(RLIZA_OBJECT);
        error->set_string(error, "error", (char *)sqlite3_errmsg(db));
        error->set_boolean(error, "success", false);
        char *json_response = (char *)rliza_dumps(error);

        size_t bytes_sent = nsock_write_all(fd,(unsigned char *)json_response,strlen(json_response)); //write_http_chunk(request, json_response, true);
        free(json_response);
        rliza_free(error);

        return bytes_sent;
    } else {
        
        free(escaped_query);

        if(params){
            for (unsigned col = 0; col < params->count; col++) {
                if (params->content.map[col]->type == RLIZA_INTEGER) {
                    sqlite3_bind_int(stmt, col + 1, params->content.map[col]->content.integer);
                } else if (params->content.map[col]->type == RLIZA_STRING) {
                    if (params->content.map[col]->content.string && *params->content.map[col]->content.string) {
                        sqlite3_bind_text(stmt, col + 1, (char *)params->content.map[col]->content.string, -1, SQLITE_STATIC);
                    } else {
                        sqlite3_bind_null(stmt, col + 1);
                    }
                } else if (params->content.map[col]->type == RLIZA_BOOLEAN) {
                    sqlite3_bind_int(stmt, col + 1, params->content.map[col]->content.boolean);
                } else if (params->content.map[col]->type == RLIZA_NULL) {
                    sqlite3_bind_null(stmt, col + 1);
                } else if (params->content.map[col]->type == RLIZA_NUMBER) {
                    sqlite3_bind_double(stmt, col + 1, params->content.map[col]->content.number);
                } else {
                    printf("Unknown parameter type: %s\n", params->content.map[col]->key);
                }
            }
        }
        rliza_t *result = rliza_new(RLIZA_OBJECT);
        result->set_boolean(result, "success", true);
        if(!sqlite3_strnicmp(query, "UPDATE", 6) || !sqlite3_strnicmp(query, "DELETE", 6) || !sqlite3_strnicmp(query, "DROP", 6)) {
            int rows_affected = sqlite3_changes(db);
            result->set_integer(result, "rows_affected", rows_affected);
        }else if(!sqlite3_strnicmp(query, "INSERT", 6)) {
            sqlite3_int64 last_insert_id = sqlite3_last_insert_rowid(db);
            if(last_insert_id)
                result->set_integer(result, "last_insert_id",last_insert_id);
        }
        char *json_response = (char *)rliza_dumps(result);
        json_response[strlen(json_response) - 1] = '\0';
        int bytes_sent = nsock_write_all(fd,json_response,strlen(json_response)); 
        free(json_response);
        if (!bytes_sent){
            nsock_close(fd);
            return 0;
        }
        long long count = 0;
        rstring_list_t * column_names = rstring_list_new();
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            rliza_t *row = rliza_new(RLIZA_ARRAY);
            if (count == 0) {
                json_response = (char *)malloc(1024 * 1024);
                json_response[0] = 0;
                strcpy(json_response, ",\"columns\":[");
                for (int col = 0; col < sqlite3_column_count(stmt); col++) {
                    char * column_name = (char *)sqlite3_column_name(stmt, col);
                    char * escaped_column_name = (char *)malloc(strlen(column_name) * 2 + 1);
                    rstraddslashes(column_name,escaped_column_name); 

                    strcat(json_response, "\"");
                    strcat(json_response, escaped_column_name);
                    rstring_list_add(column_names,escaped_column_name);

                    free(escaped_column_name);
                    strcat(json_response, "\",");
                }
                if (json_response[strlen(json_response) - 1] == ',')
                    json_response[strlen(json_response) - 1] = ']';
                strcat(json_response, ",\"rows\":[");
                bytes_sent = nsock_write_all(fd,(unsigned char *)json_response,strlen(json_response));
                free(json_response);
                rliza_free(result);
                if(bytes_sent == 0){
                    nsock_close(fd);
                    return 0;
                }
            }
            for (int col = 0; col < sqlite3_column_count(stmt); col++) {

            
              //  params->content.map[col]->type = RLIZA_STRING;
                //char * column_name = column_names->strings[col];
                //printf("GOT COLLL!\n");
                int type = sqlite3_column_type(stmt, col);
                switch (type) {
                case SQLITE_INTEGER:
                
                    rliza_push(row, rliza_new_integer((long long)sqlite3_column_int(stmt, col)));
                    break;

                case SQLITE_FLOAT:
                    rliza_push(row, rliza_new_number(sqlite3_column_double(stmt, col)));
                    break;
                case SQLITE_TEXT:
                    char *string = (char *)sqlite3_column_text(stmt, col);
                    if (string && !strcmp(string, "true")) {
                        rliza_push(row, rliza_new_boolean(true));
                    } else if (string && !strcmp(string, "false")) {
                        rliza_push(row, rliza_new_boolean(false));
                    } else {
                        rliza_push(row, rliza_new_string(string));
                    }
                    break;
                case SQLITE_BLOB:
                    break;
                case SQLITE_NULL:
                    rliza_push(row, rliza_new_null());
                    break;
                default:
                    printf("Unknown column type\n");
                }
            }
            
            count++;
            json_response = (char *)rliza_dumps(row);
            char *prefixed_json_response = (char *)malloc(strlen(json_response) + 1024);
            prefixed_json_response[0] = 0;
            if (count > 1)
                strcpy(prefixed_json_response, ",");
            strcat(prefixed_json_response, json_response);
            free(json_response);
            size_t bytes_sent_chunk = nsock_write_all(fd, (unsigned char*)prefixed_json_response, strlen(prefixed_json_response));
            bytes_sent += bytes_sent_chunk;
            free(prefixed_json_response);
            rliza_free(row);
            if (!bytes_sent_chunk)
                return 0;
            
        }
        rstring_list_free(column_names);
            column_names = NULL;
        if(count == 0)
            rliza_free(result);
        sqlite3_finalize(stmt);
        char *rows_end = strdup(count ? "]" : "");
        char *footer_end = (char *)malloc(512);
        sprintf(footer_end, ",\"count\":%lld}", count);
        json_response = (char *)malloc(1024);
        sprintf(json_response, "%s,%s\r\n", rows_end, footer_end + 1);
        free(rows_end);
        free(footer_end);
        size_t bytes_sent_chunk = nsock_write_all(fd, (unsigned char *)json_response, strlen(json_response));
        bytes_sent += bytes_sent_chunk;
        free(json_response);
        if (bytes_sent_chunk == 0)
            return 0;
        return bytes_sent;
    }
    return 0;
}

ulonglong get_system_int(char * name) {
    sqlite3_stmt * stmt;
    char * sql = "SELECT value_int FROM stogram WHERE name = ?;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_ROW){
        raise_db_error();
    }
    ulonglong value = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return value;
}

char * get_system_string(char * name) {
    sqlite3_stmt * stmt;
    char * sql = "SELECT value_string FROM stogram WHERE name = ?;";
    if(sqlite3_prepare(db, sql, -1, &stmt, NULL) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 0, name, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_ROW){
        raise_db_error();
    }
    char * result = sbuf((char *)sqlite3_column_text(stmt, 1));
    sqlite3_finalize(stmt);
    return result;
}
void set_system_string(char * name, char * value) {
    sqlite3_stmt * stmt;
    char * sql = "UPDATE stogram SET value_string = ? WHERE name = ?";
    if(sqlite3_prepare(db, sql, -1, &stmt, NULL) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 0, value, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_OK){
        raise_db_error();
    }
}

void set_system_int(char * name, ulonglong value) {
    sqlite3_stmt * stmt = 0;
    char * sql = "UPDATE stogram SET value_int = ? WHERE name = ?;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_int64(stmt, 1, value);
        sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
    }
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void drop_schema() {
    char * sql = "DROP TABLE IF EXISTS subscriptions;";
    if(sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK){
        raise_db_error();
    }
    sql = "DROP TABLE IF EXISTS subscribers;";
    if(sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK){
        raise_db_error();
    }
}

bool table_exists(char * name){
    char * sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt * stmt;
    if(sqlite3_prepare(db, sql, -1, &stmt, NULL) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) == SQLITE_ROW){
        sqlite3_finalize(stmt);
        return true;
    }else{
        sqlite3_finalize(stmt);
        return false;
    }
}

ulonglong pk(){
    ulonglong val = get_system_int("pk");
    val++;
    set_system_int("pk", val);
    return val;
}
char * ruid(){
    char temp[20];
    sprintf(temp, "%X", (unsigned int)pk());
    return sbuf(temp);
}

void db_index(char *table_name, char *field_name){
    char sql[1024] = {0};
    sprintf(sql, "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);", table_name, field_name, table_name, field_name);
    if(sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK){
        raise_db_error();
    }
}
void db_execute(char * sql){
    sqlite3_stmt * stmt;
    if(sqlite3_prepare_v2(db, sql, -1,&stmt,0) != SQLITE_OK){
        raise_db_error();
    }
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

char * pstr(const char * str, ...){
    va_list args;
    va_start(args, str);
    char result[4096] = {0};
    vsnprintf(result, sizeof(result), str, args);
    return sbuf(result);
}

void create_schema(){
    char * default_columns = "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "created_date TEXT DEFAULT(date('now')),"
         "created_time TEXT DEFAULT(time('now')),"
         "created_datetime TEXT DEFAULT(datetime('now')),"
         "uid TEXT NULL";
    sqlite3_stmt * stmt;
    bool is_new_db = !table_exists("stogram");
    if(is_new_db){
        db_execute(pstr("CREATE TABLE stogram (%s, name text, value_int integer NULL, value_str text NULL);", default_columns));
        db_index("stogram", "created_date");
        db_index("stogram", "name");
        db_execute(pstr("INSERT INTO stogram (name,value_int) VALUES (\"pk\", 1337);"));
    }
    db_execute(pstr("CREATE TABLE IF NOT EXISTS published(%s, published_by TEXT, topic TEXT, message TEXT);",default_columns));
    db_index("published", "created_date");
    db_index("published", "topic");
    db_index("published", "published_by");
    db_execute(pstr("CREATE TABLE IF NOT EXISTS subscriptions (%s,subscriber TEXT, subscribed_to text);",default_columns));
    db_index("subscriptions", "created_date");
    db_index("subscriptions", "subscriber");
    db_index("subscriptions", "subscribed_to");
    db_execute(pstr("CREATE TABLE IF NOT EXISTS subscribers (%s,"
            "last_active_date text default(date('now')),"
            "last_active_time text default(time('now')),"
            "last_active_datetime text default(datetime('now')),"
            "last_message_id integer NULL,"
            "name TEXT,"
            "fd int);",default_columns));
    db_index("subscribers", "created_date");
    db_index("subscribers", "name");
    db_index("subscribers", "fd");
}

rstring_list_t * get_subscribed_to(char * subscribed_to){
    rstring_list_t * list = rstring_list_new();
    sqlite3_stmt * stmt = 0;
    char * sql = "SELECT subscriber FROM subscriptions WHERE subscribed_to = ? and fd > 0;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, subscribed_to, -1, SQLITE_STATIC);
    }
    while(sqlite3_step(stmt) == SQLITE_ROW){
        rstring_list_add(list, sbuf((char *)sqlite3_column_text(stmt, 0)));
    }
    sqlite3_finalize(stmt);
    return list;
}

bool update_subscriber(char * name, int fd){
    sqlite3_stmt * stmt = 0;
    char * sql = "UPDATE subscribers SET fd = ? WHERE name = ?;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0)){
        raise_db_error();
    }else{
        sqlite3_bind_int(stmt, 1, fd);
        sqlite3_bind_text(stmt,2, name,-1,SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_DONE){
        raise_db_error();
    }
    sqlite3_finalize(stmt);
    return sqlite3_changes(db) > 0;
}

int get_subscriber_fd_by_name(char * name){
    sqlite3_stmt * stmt = 0;
    char * sql = "SELECT fd FROM subscribers WHERE name = ? and fd > 0;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    }
    int result = 0;
    if(sqlite3_step(stmt) == SQLITE_ROW){
        result = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return result;
}

void unset_subscriber_fd(int fd){
    db_execute(pstr("UPDATE subscribers SET fd = NULL WHERE fd = %d;",fd));
}
char * get_subscriber_by_fd(int fd){
    sqlite3_stmt * stmt = 0;
    char * sql = "SELECT name FROM subscribers WHERE fd = ?;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_int(stmt, 1, fd);
    }
    char * name = 0;
    if(sqlite3_step(stmt) == SQLITE_ROW){
        name = sbuf((char *)sqlite3_column_text(stmt, 0));
    }
    sqlite3_finalize(stmt);
    return name;
}

rstring_list_t * get_subscribers_to(char * topic){
    sqlite3_stmt * stmt = 0;
    rstring_list_t * result = rstring_list_new();
    char * sql = "SELECT subscriber FROM subscriptions INNER JOIN subscribers on (subscriptions.subscriber = subscribers.name and subscribers.fd > 0) where subscribed_to = ? GROUP BY subscriber;;";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        
        raise_db_error();
    }else{ 
        sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    }
    while(sqlite3_step(stmt) == SQLITE_ROW){
        rstring_list_add(result, (char *)sqlite3_column_text(stmt, 0));
    }
    return result;
}

ulonglong insert_publish(char * published_by, char * topic, char * message){
    sqlite3_stmt * stmt = 0;
    char * str ="INSERT INTO published (published_by,topic,message) VALUES (?,?,?);";
    if(sqlite3_prepare_v2(db, str, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, published_by, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, topic, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, message, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_DONE){
        raise_db_error();
    }
    sqlite3_finalize(stmt);
    return sqlite3_last_insert_rowid(db);
}

void insert_subscriber(char *name, int fd){
    sqlite3_stmt * stmt = 0;
    char * uid = ruid();
    char * sql = "INSERT INTO subscribers"
        "(uid, name, fd,last_active_date,last_active_time,last_active_datetime)"
        " VALUES "
        "(?, ?, ?,date('now'),time('now'),datetime('now'));";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, uid, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 3, fd);
    }
    if(sqlite3_step(stmt) != SQLITE_DONE){
        raise_db_error();
    }
    sqlite3_finalize(stmt);
}
void register_subscriber(char * name, int fd){
    if(!update_subscriber(name, fd))
        insert_subscriber(name,fd);
}

int subscribe(char * subscriber, char * subscribe_to){
    sqlite3_stmt * stmt = 0;
    char * uid = ruid();
    char * sql = "INSERT INTO subscriptions (uid, subscriber, subscribed_to) VALUES (?, ?, ?);";
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK){
        raise_db_error();
    }else{
        sqlite3_bind_text(stmt, 1, uid, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, subscriber, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, subscribe_to, -1, SQLITE_STATIC);
    }
    if(sqlite3_step(stmt) != SQLITE_DONE){
        raise_db_error();
    }
    sqlite3_finalize(stmt);
    return sqlite3_last_insert_rowid(db);
}
#endif
