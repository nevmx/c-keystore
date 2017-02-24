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
    // Ensure that the store has been created
    if (kv_store_created != 1) {
        return -1;
    }
    
    if (!key || !value || strlen(key) < 1 || strlen(value) < 1) {
        return -1;
    }
    
    char key_s[32];
    
    strncpy(key_s, key, 32);
    key_s[31] = '\0';
    

    int index = hash_function(key_s);
    char *pod_addr = store_addr + (index * POD_SIZE);
    
    // Write key
    memcpy(pod_addr, key_s, KEY_SIZE);
    
    // Write value
    memcpy(pod_addr + KEY_SIZE, value, VALUE_SIZE);
    
    return 0;
}

char *kv_store_read(char *key) {
    // Ensure that the store has been created
    if (kv_store_created != 1) {
        return NULL;
    }
    
    if (!key || strlen(key) < 1) {
        return NULL;
    }
    
    char key_s[32];
    
    strncpy(key_s, key, 32);
    key_s[31] = '\0';
    
    int index = hash_function(key_s);
    char *pod_addr = store_addr + (index * POD_SIZE);
    
    char *value = (void*)calloc(sizeof(char), VALUE_SIZE);
    memcpy(value, pod_addr + KEY_SIZE, VALUE_SIZE);
    return value;
}

int main(int argc, char** argv) {
    int result1 = kv_store_create("some_store");
    //(void)kv_store_write("0123456789012345678901234567890123456789", "greater_than_32");
    (void)kv_store_write("01234567890123456789012345678901", "exactly_32");
    (void)kv_store_write("sad9", "less_than_32");
    
    //char* value1 = kv_store_read("0123456789012345678901234567890123456789");
    char* value2 = kv_store_read("01234567890123456789012345678901");
    char* value3 = kv_store_read("sad9");
    
    //printf("Value 1: %s\n", value1);
    printf("Value 2: %s\n", value2);
    printf("Value 3: %s\n", value3);
    //free(value1);
    free(value2);
    free(value3);
}

