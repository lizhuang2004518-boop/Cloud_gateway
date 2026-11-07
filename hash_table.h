#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>


// 声明哈希节点结构体类型
typedef struct HashNode {
    char* key;
    void* value;
    struct HashNode* next;
} HashNode;


// 声明哈希表结构体类型
typedef struct HashTable {
    HashNode** buckets;     // 哈希桶数组
    size_t size;            // 哈希表中元素的数量
    size_t capacity;        // 哈希表的容量
} HashTable;


extern HashTable* ci_table;


HashTable* create_hash_table(size_t capacity);
void hash_table_insert(HashTable* table, const char* key, void* value);
int hash_table_remove(HashTable* table, const char* key);
void* hash_table_get(HashTable* table, const char* key);
size_t hash_table_size(HashTable* table);
int hash_table_empty(HashTable* table);
void destroy_hash_table(HashTable* table);



#endif // HASH_TABLE_H
