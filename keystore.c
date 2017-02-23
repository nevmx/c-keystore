#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

/*
    32 pods.
    1 key: 32 bytes, 1 value: 256 bytes.
    
    32*(32+256) = 9,216 bytes
*/

#define STORE_SIZE 9216
#define KEY_SIZE 32
#define VALUE_SIZE 256

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
    int index = hash_function(key);
}

int main(int argc, char** argv) {
    
    char* str = argv[1];
    
    printf("%s: %d\n", str, hash_function(str));
}

