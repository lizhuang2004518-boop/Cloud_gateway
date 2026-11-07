/*
 * hash_table.c —— 在线设备表（key = 设备ID，value = socket）
 *
 * FIX #3：原版三个操作完全无锁。而 device_server.c 是"一个连接一个线程"，
 *         mqtt_client.c 的 messageArrived 回调线程也会调用 hash_table_get，
 *         并发 insert/remove/get 会遍历到已被 free 的节点 → 偶发段错误。
 *         这里用一把互斥锁把表操作串行化（-pthread 已在 Makefile 中）。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "hash_table.h"


HashTable* ci_table = NULL;

/* 保护表结构的全局锁 */
static pthread_mutex_t g_table_lock = PTHREAD_MUTEX_INITIALIZER;


// 创建哈希表（启动阶段调用，此时还没有线程，不需要加锁）
HashTable* create_hash_table(size_t capacity) 
{
    HashTable* table = (HashTable*)malloc(sizeof(HashTable));

    if (!table) 
    {
        return NULL;
    }

    table->buckets = (HashNode**)calloc(capacity, sizeof(HashNode*));

    if (!table->buckets) 
    {
        free(table);
        return NULL;
    }

    table->size = 0;
    table->capacity = capacity;

    return table;
}


// 哈希函数（经典实现）
static size_t hash_function(const char* key, size_t capacity)
{
	// 假如 key 为 "abc"，那么
	// r = ('a' * 31^2 + 'b' * 31^1 + 'c' * 31^0) % capacity;
	size_t r = 0, i;

	for(i = 0; key[i] != '\0'; i++)
	{
		r = r * 31 + (size_t)(unsigned char)key[i];
	}

	r %= capacity;

	return r;
}


// 在哈希表中插入元素
void hash_table_insert(HashTable* table, const char* key, void* value)
{
	size_t pos;
	HashNode* p;
	HashNode* node;

	if(table == NULL || key == NULL)
	{
		return;
	}

	pthread_mutex_lock(&g_table_lock);

	pos = hash_function(key, table->capacity);
	p = table->buckets[pos];

	while(p != NULL)
	{
		// 如果 key 已经存在，就更新它对应的 value
		if(strcmp(p->key, key) == 0)
		{
			p->value = value;
			pthread_mutex_unlock(&g_table_lock);
			return;
		}
		
		p  = p->next;
	}

	// 如果 key 不存在，就在哈希桶中插入新节点
	node = (HashNode*)malloc(sizeof(HashNode));

	if(node == NULL)
	{
		pthread_mutex_unlock(&g_table_lock);
		perror("malloc");
		return;
	}

	node->key = strdup(key);

	if(node->key == NULL)
	{
		free(node);
		pthread_mutex_unlock(&g_table_lock);
		perror("strdup");
		return;
	}

	node->value = value;
	node->next = table->buckets[pos];

	table->buckets[pos] = node;

	table->size++;

	pthread_mutex_unlock(&g_table_lock);
}


// 在哈希表中删除元素
int hash_table_remove(HashTable* table, const char* key)
{
	size_t pos;
	HashNode* p, *q = NULL;
	int ret = 0;

	if(table == NULL || key == NULL)
	{
		return 0;
	}

	pthread_mutex_lock(&g_table_lock);

	pos = hash_function(key, table->capacity);
	p = table->buckets[pos];

	while(p != NULL)
	{
		// 如果找到了 key，就删除它并返回 1
		if(strcmp(p->key, key) == 0)
		{
			if(q != NULL)
				q->next = p->next;
			else
				table->buckets[pos] = p->next;

			free(p->key);
			free(p);

			table->size--;

			ret = 1;
			break;
		}
		
		q = p;
		p  = p->next;
	}

	pthread_mutex_unlock(&g_table_lock);

	// 如果未找到 key 就返回 0，表示删除失败
	return ret;
}


// 在哈希表中查找元素
void* hash_table_get(HashTable* table, const char* key)
{
	size_t pos;
	HashNode* p;
	void* ret = NULL;

	if(table == NULL || key == NULL)
	{
		return NULL;
	}

	pthread_mutex_lock(&g_table_lock);

	pos = hash_function(key, table->capacity);

	for(p = table->buckets[pos]; p != NULL; p = p->next)
	{
		// 如果 key 存在，就返回它对应的 value
		if(strcmp(p->key, key) == 0)
		{
			ret = p->value;
			break;
		}
	}

	pthread_mutex_unlock(&g_table_lock);

	// 如果 key 不存在，就返回 NULL
	return ret;
}


// 计算哈希表的长度
size_t hash_table_size(HashTable* table)
{
	size_t n;

	if(table == NULL)
	{
		return 0;
	}

	pthread_mutex_lock(&g_table_lock);
	n = table->size;
	pthread_mutex_unlock(&g_table_lock);

	return n;
}


// 判断哈希表是否为空
int hash_table_empty(HashTable* table)
{
	return !hash_table_size(table);
}


// 销毁哈希表（正常流程里不会走到，进程退出时由系统回收）
void destroy_hash_table(HashTable* table) 
{
    HashNode* head = NULL, *temp = NULL;
    size_t i;

    if (!table) 
    {
        return;
    }

    pthread_mutex_lock(&g_table_lock);

    for (i = 0; i < table->capacity; i++) 
    {
        head = table->buckets[i];

        while(head != NULL) 
        {
            temp = head;
            head = head->next;
            free(temp->key);
            free(temp);
        }
    }

    free(table->buckets);
    free(table);

    pthread_mutex_unlock(&g_table_lock);
}
