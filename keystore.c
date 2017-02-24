#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <semaphore.h>

/*
    256 pods. each pod 8 units deep
    1 key: 32 bytes, 1 value: 256 bytes.
    1 KVP = 288 bytes.
    1 pod = 288 bytes * 8 units = 2,304 bytes
    256 pods = 2,304 * 256 = 589,824
    
*/

#define STORE_SIZE 589824
#define KEY_SIZE 32
#define VALUE_SIZE 256
#define POD_SIZE 2304
#define N_OF_PODS 256
#define POD_DEPTH 8

char *store_addr = NULL;
int kv_store_created = 0;
sem_t *keystore_semaphore[N_OF_PODS];

/*
    Source: http://stackoverflow.com/questions/20462826/hash-function-for-strings-in-c
*/

int init_semaphores() {
    char name[25];
    for (int i = 0; i < N_OF_PODS; i++) {
        sprintf(name, "ecse427-maximneverov-%i", i);
        keystore_semaphore[i] = sem_open(name, O_CREAT|O_RDWR, S_IRWXU, 1);
        
        if (keystore_semaphore[i] == SEM_FAILED) {
            return -1;
        }
    }
    return 0;
}

int hash_function(char* key)
{
    unsigned int hash = 0;
    for (int i = 0 ; key[i] != '\0' ; i++)
    {
        hash = 31*hash + key[i];
    }
    return hash % N_OF_PODS;
}

// This function will empty the store - all keys will be NULL
void init_store() {
    char empty[] = "";
    for (int i = 0; i < N_OF_PODS; i++) {
        for (int j = 0; j < POD_DEPTH; j++) {
            memcpy(store_addr + (i * POD_SIZE) + (j * (KEY_SIZE + VALUE_SIZE)), empty, sizeof(empty));
        }
    }
}

int kv_store_create(char *name) {
    int fd = shm_open(name, O_CREAT|O_RDWR, S_IRWXU);
    
    if (fd < 0) {
        return -1;
    }
    
    store_addr = mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ftruncate(fd, STORE_SIZE);
    close(fd);
    
    init_store();
    
    if (init_semaphores() != 0) {
        return -1;
    }
    
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
    
    // Truncate the key to 32 bytes. Last byte must be string terminator.
    char key_s[KEY_SIZE];
    strncpy(key_s, key, KEY_SIZE);
    key_s[KEY_SIZE - 1] = '\0';
    
    // Truncate the value to 256 bytes. Last byte must be string terminator.
    char value_s[VALUE_SIZE];
    strncpy(value_s, value, VALUE_SIZE);
    value_s[VALUE_SIZE - 1] = '\0';
    
    // Find the index, and address of the pod
    int index = hash_function(key_s);
    char *pod_addr = store_addr + (index * POD_SIZE);
    
    // Find an empty spot in this pod
    char *mem_loc = pod_addr;
    char empty[] = "";
    while (strcmp(mem_loc, empty) != 0 && (mem_loc - pod_addr)/(KEY_SIZE+VALUE_SIZE) < POD_DEPTH) {
        mem_loc += (KEY_SIZE + VALUE_SIZE);   
    }
    
    
    // If the pod if full - we need to remove the oldest element in this pod and replace it with the new element
    if ((mem_loc - pod_addr)/(KEY_SIZE+VALUE_SIZE) > POD_DEPTH - 1) {
        // First, move all elements back by one - O(n) run time :(
        
        // Return pointer to beginning of pod
        mem_loc = pod_addr;
        
        while ((mem_loc - pod_addr)/(KEY_SIZE+VALUE_SIZE) < POD_DEPTH - 1) {
            // Move the key and value pair
            memcpy(mem_loc, mem_loc + (KEY_SIZE+VALUE_SIZE), KEY_SIZE+VALUE_SIZE);
            mem_loc += KEY_SIZE+VALUE_SIZE;
        }
        
        // After copying all the pairs, insert new one in the last slot
    }
    
    // Write key
    memcpy(mem_loc, key_s, KEY_SIZE);
    
    // Write value
    memcpy(mem_loc + KEY_SIZE, value_s, VALUE_SIZE);
    
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
    
    // Truncate the key to 32 bytes. Last byte must be string terminator.
    char key_s[KEY_SIZE];
    strncpy(key_s, key, KEY_SIZE);
    key_s[KEY_SIZE - 1] = '\0';
    
    int index = hash_function(key_s);
    char *pod_addr = store_addr + (index * POD_SIZE);
    char *mem_loc = pod_addr;
    
    char empty[] = "";
    while (strcmp(mem_loc, key_s) != 0 && (mem_loc - pod_addr)/(KEY_SIZE+VALUE_SIZE) < POD_DEPTH) {
        if (strcmp(mem_loc, empty) == 0) {
            return NULL;
        }
        mem_loc += (KEY_SIZE + VALUE_SIZE);
    }
    
    char *value = (void*)calloc(VALUE_SIZE, sizeof(char));
    memcpy(value, mem_loc + KEY_SIZE, VALUE_SIZE);
    return value;
}

char **kv_store_read_all(char *key) {
    // Ensure that the store has been created
    if (kv_store_created != 1) {
        return NULL;
    }
    
    if (!key || strlen(key) < 1) {
        return NULL;
    }
    
    // Truncate the key to 32 bytes. Last byte must be string terminator.
    char key_s[KEY_SIZE];
    strncpy(key_s, key, KEY_SIZE);
    key_s[KEY_SIZE - 1] = '\0';
    
    int index = hash_function(key_s);
    char *pod_addr = store_addr + (index * POD_SIZE);
    char *mem_loc = pod_addr;
    char empty[] = "";
    
    char **result = (void*)calloc(POD_DEPTH, sizeof(char *)); // Maximum number of matches is the actual depth of the pod
    
    // Scan the pod until you reach an empty key or end of pod
    int result_index = 0;
    while (strcmp(mem_loc, empty) != 0 && (mem_loc - pod_addr)/(KEY_SIZE+VALUE_SIZE) < POD_DEPTH) {
        // If key matches, it needs to be returned
        if (strcmp(mem_loc, key_s) == 0) {
        
            // Allocate memory before copying the value in.
            result[result_index] = (void*)calloc(VALUE_SIZE, sizeof(char));
            
            // Copy the value
            memcpy(result[result_index], mem_loc + KEY_SIZE, VALUE_SIZE);
            
            result_index++;
        }
        mem_loc += KEY_SIZE+VALUE_SIZE;
    }
    
    // Resize result so that it's not any bigger than the amount of elements in it
    if (result_index < 8)
        result = (void *)realloc(result, (result_index + 1) * sizeof(char *));
    
    // This array will be null-terminated so that the user knows the size of it.
    // There is no other way to know the size.
    result[result_index] = NULL;
    
    return result;
}

int main(int argc, char** argv) {
    (void)kv_store_create("some_store");
    (void)kv_store_write("student_id", "260621662 - 1");
    (void)kv_store_write("student_id", "P00P - 2");
    (void)kv_store_write("student_id", "This is but a test. - 3");
    (void)kv_store_write("student_id", "This is but a test. - 4");
    (void)kv_store_write("student_id", "This is but a test. - 5");
    (void)kv_store_write("student_id", "This is but a test. - 6");
    (void)kv_store_write("student_id", "This is but a test. - 7");
    (void)kv_store_write("student_id", "This is but a test. - 8");
    (void)kv_store_write("student_id", "This is but a test. - 9");
    (void)kv_store_write("student_id", "OMG so full...");
    (void)kv_store_write("school", "mcgill1");
    (void)kv_store_write("school", "mcgill2");
    (void)kv_store_write("school", "mcgill3");
    (void)kv_store_write("ah", "Maxim Neverov");
    (void)kv_store_write("ah", "Maxim NNNNNev");
    
    char* value1 = kv_store_read("student_id");
    char* value2 = kv_store_read("school");
    char* value3 = kv_store_read("ah");
    
    char** values = kv_store_read_all("student_id");
    
    printf("Value 1: %s\n", value1);
    printf("Value 2: %s\n", value2);
    printf("Value 3: %s\n\n", value3);
    
    for (int i = 0; i < 8 && values[i] != NULL; i++) {
        printf("Values: %s\n", values[i]);
    }
    
    free(value1);
    free(value2);
    free(value3);
    free(values);
}

