#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

/*
    32 pods.
    1 key: 32 bytes, 1 value: 256 bytes.
    
    32*(32+256) = 9,216 bytes
*/

#define STORE_SIZE 9216
#define KEY_SIZE 32
#define VALUE_SIZE 256
#define POD_SIZE 288

char *store_addr = NULL;
int kv_store_created = 0;

/*
    Source: http://stackoverflow.com/questions/20462826/hash-function-for-strings-in-c
*/

int hash_function(char* key)
{
    unsigned int hash = 0;
    for (int i = 0 ; key[i] != '\0' ; i++)
    {
        hash = 31*hash + key[i];
    }
    return hash % 32;
}

int kv_store_create(char *name) {
    int fd = shm_open(name, O_CREAT|O_RDWR, S_IRWXU);
    
    if (fd < 0) {
        return -1;
    }
    
    store_addr = mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ftruncate(fd, STORE_SIZE);
    close(fd);
    
    kv_store_created = 1;
    
    return 0;
}

int kv_store_write(char *key, char *value) {
    if (kv_store_created != 1) {
        return -1;
    }

    int index = hash_function(key);
    char *pod_addr = store_addr + (index * POD_SIZE);
    
    // Write key
    memcpy(pod_addr, key, KEY_SIZE);
    
    // Write value
    memcpy(pod_addr + KEY_SIZE, value, VALUE_SIZE);
    
    return 0;
}

char *kv_store_read(char *key) {
    if (kv_store_created != 1) {
        return NULL;
    }
    
    int index = hash_function(key);
    char *pod_addr = store_addr + (index * POD_SIZE);
    
    char *value = (void*)calloc(sizeof(char), VALUE_SIZE);
    memcpy(value, pod_addr + KEY_SIZE, VALUE_SIZE);
    return value;
}

int main(int argc, char** argv) {
    int result1 = kv_store_create("some_store");
    int result = kv_store_write("key", "value_hehehehe");
    
    char* value = kv_store_read("key");
    
    printf("Value: %s\n", value);
    free(value);
}

