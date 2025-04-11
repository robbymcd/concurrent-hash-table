// Hayden Sandler

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include "rwlock.c" // Custom reader-writer lock implementation

#define MAX_NAME 50
#define MAX_COMMANDS 100

// Struct representing a record in the hash table
typedef struct hash_struct {
    uint32_t hash;
    char name[MAX_NAME];
    uint32_t salary;
    struct hash_struct *next;
} hashRecord;

hashRecord *head = NULL;      // Head of the hash record linked list
rwlock_t rwlock;              // Reader-writer lock instance

FILE *output;                 // Output file pointer

// Struct representing a parsed command from the input file
typedef struct {
    char command[10];         // "insert", etc.
    char name[MAX_NAME];      // Name of the person
    uint32_t salary;          // Associated salary
} Command;

Command commands[MAX_COMMANDS];  // Array to hold parsed commands
int command_count = 0;           // Number of parsed commands

// Hash function for strings
uint32_t oneTimeHash(const char *key) {
    uint32_t hash = 0;
    while (*key) {
        hash += (unsigned char)(*key++);
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

// Returns current timestamp (seconds since epoch)
long long current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec;
}

// Inserts a record into the hash table (thread-safe via write lock)
void insert_record(const char *name, uint32_t salary) {
    uint32_t h = oneTimeHash(name);

    rwlock_acquire_writelock(&rwlock); // Exclusive access

    // Search if the record already exists
    hashRecord *curr = head;
    while (curr) {
        if (curr->hash == h && strcmp(curr->name, name) == 0) {
            // If record exists, update salary
            curr->salary = salary;
            rwlock_release_writelock(&rwlock);
            return;
        }
        curr = curr->next;
    }

    // Otherwise, insert new record at the head of the list
    hashRecord *newNode = malloc(sizeof(hashRecord));
    newNode->hash = h;
    strncpy(newNode->name, name, MAX_NAME);
    newNode->salary = salary;
    newNode->next = head;
    head = newNode;

    rwlock_release_writelock(&rwlock); // Release exclusive access
}

// Thread entry point to execute a command
void *execute_command(void *arg) {
    int idx = *(int *)arg;
    free(arg);  // Free the dynamically allocated thread argument
    Command cmd = commands[idx];

    if (strcmp(cmd.command, "insert") == 0) {
        fprintf(output, "%lld,INSERT,%s,%d\n", current_timestamp(), cmd.name, cmd.salary);
        insert_record(cmd.name, cmd.salary);
    }

    return NULL;
}

// Reads and parses commands from "commands.txt"
void parse_commands() {
    FILE *fp = fopen("commands.txt", "r");
    if (!fp) {
        perror("Error opening commands.txt");
        exit(1);
    }

    char line[256];
    printf("Opened commands.txt successfully.\n");

    while (fgets(line, sizeof(line), fp)) {
        // Parse CSV line: command,name,salary
        char *cmd = strtok(line, ",\n");
        char *name = strtok(NULL, ",\n");
        char *salary_str = strtok(NULL, ",\n");

        if (!cmd || !name || !salary_str) continue;

        if (strncmp(cmd, "threads", 7) == 0) continue; // Skip header line if present

        // Store parsed values in command array
        strncpy(commands[command_count].command, cmd, sizeof(commands[command_count].command));
        strncpy(commands[command_count].name, name, MAX_NAME);
        commands[command_count].salary = atoi(salary_str);
        command_count++;
    }

    fclose(fp);
    printf("Total commands parsed: %d\n", command_count);
}

int main() {
    rwlock_init(&rwlock);  // Initialize the custom read-write lock

    output = fopen("output.txt", "w");
    if (!output) {
        perror("Error opening output.txt");
        return 1;
    }

    parse_commands(); // Load commands from file

    if (command_count == 0) {
        fprintf(output, "No commands found to execute.\n");
        fclose(output);
        return 0;
    }

    // Create one thread per command
    pthread_t threads[command_count];
    for (int i = 0; i < command_count; i++) {
        int *arg = malloc(sizeof(int)); // Allocate index as thread argument
        *arg = i;
        pthread_create(&threads[i], NULL, execute_command, arg);
    }

    // Wait for all threads to complete
    for (int i = 0; i < command_count; i++) {
        pthread_join(threads[i], NULL);
    }

    // Sort the hash table entries by hash for output
    hashRecord *sorted = NULL, *curr = head;
    while (curr) {
        hashRecord *next = curr->next;

        // Insert into sorted list based on hash
        if (!sorted || curr->hash < sorted->hash) {
            curr->next = sorted;
            sorted = curr;
        } else {
            hashRecord *s = sorted;
            while (s->next && s->next->hash < curr->hash) s = s->next;
            curr->next = s->next;
            s->next = curr;
        }

        curr = next;
    }

    // Print sorted records to output file
    curr = sorted;
    while (curr) {
        fprintf(output, "%u,%s,%u\n", curr->hash, curr->name, curr->salary);
        curr = curr->next;
    }

    fclose(output); // Close the output file
    return 0;
}
