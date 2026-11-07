/*
 * mqtt_client.c —— MQTT 客户端（Paho MQTTAsync）
 *
 * 本文件相对原版的改动（都标了 FIX）：
 *   FIX #7：clientId 改成可配置的唯一值，避免和别的实例/老师环境撞 ID 互相踢下线
 *   FIX #8：首次连接失败会自动重试（Paho 的 automaticReconnect 只对"连上过之后断开"生效，
 *           首次连不上原版是静默不工作）
 *   FIX #9：mqttclient_pub_msg 里的 static 发送缓冲改成局部变量，多设备并发发送时不会互相踩
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "MQTTAsync.h"
#include "hash_table.h"

/* 你的 broker 和本程序在同一台机器上，走回环最快最稳。
 * 不要改成公网 IP：会绕出去再回来，很多云主机的 NAT 回环（hairpin）还不支持。 */
#define ADDRESS_ITMOJUN         "tcp://127.0.0.1:1883"

/* FIX #7：同一 broker 上 clientId 必须唯一，否则后连上的会把先连上的踢掉。
 *         如果以后要跑多个实例，给每个实例起不同名字（或加随机后缀）。 */
#ifndef CLIENTID_ITMOJUN
#define CLIENTID_ITMOJUN        "device_cloud_main"
#endif


#define QOS             1
#define MQTT_CONNECTED 1
#define MQTT_DISCONNECTED 0 



MQTTAsync g_client_itmojun;
static volatile int g_connected_itmojun = MQTT_DISCONNECTED;


/*
 * MQTT 发布相关回调与发送函数。
 * 作用是把来自无线设备的消息转发到 MQTT Broker，供云端/其他系统订阅。
 */

static void onSendFailure(void* context, MQTTAsync_failureData* response)
{
    printf("Message send failed token %d error code %d\n", response->token, response->code);
}


static void onSend(void* context, MQTTAsync_successData* response)
{
    // This gets called when a message is acknowledged successfully.
}


/*
 * mqttclient_pub_msg()
 * 将设备上报数据发布到 MQTT 主题中。
 * 参数：
 *   client - MQTT 连接对象
 *   topic  - 设备主题，例如 device123/temp
 *   payload - 设备消息内容，例如 25.6
 *
 * 典型调用链：
 *   client_communication() -> mqttclient_pub_msg() -> MQTTAsync_sendMessage()
 */
void mqttclient_pub_msg(MQTTAsync* client, const char* topic, const char* payload)
{
    /* FIX #9：原来是 static，多个设备线程同时调用会互相覆盖 payload/qos，
     *         偶尔发出错乱的消息。改成每次调用各自一份。 */
    MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
    MQTTAsync_responseOptions pub_opts = MQTTAsync_responseOptions_initializer;
    int rc;

    int connect_state = g_connected_itmojun;

    if(MQTT_CONNECTED == connect_state)
    {
        pub_opts.onSuccess = onSend;
        pub_opts.onFailure = onSendFailure;
        pub_opts.context = *client;

        pubmsg.payload = (void*)payload;
        pubmsg.payloadlen = strlen(payload);
        pubmsg.qos = QOS;
        pubmsg.retained = 0;

        if ((rc = MQTTAsync_sendMessage(*client, topic, &pubmsg, &pub_opts)) != MQTTASYNC_SUCCESS)
        {
            fprintf(stderr, "Failed to start sendMessage, return code %d\n", rc);
            //g_connected = MQTT_DISCONNECTED;
        }
    }
    else
    {
        fprintf(stderr, "mqtt disconnected...\n");
    }
}



static void onSubscribe(void* context, MQTTAsync_successData* response)
{
	printf("Subscribe succeeded\n");
	//subscribed = 1;
}


static void onSubscribeFailure(void* context, MQTTAsync_failureData* response)
{
	printf("Subscribe failed, rc %d\n", response->code);
}


static void onConnect(void* context, MQTTAsync_successData* response)
{
    printf("Successful connection\n");

    g_connected_itmojun = MQTT_CONNECTED;
    
}


static void onConnectFailure(void* context, MQTTAsync_failureData* response)
{
    printf("Connect failed, rc %d\n", response ? response->code : 0);
 
    g_connected_itmojun = MQTT_DISCONNECTED;
}


static void connlost(void *context, char *cause)
{
    printf("\nERROR:Connection lost,Cause: %s,Reconnecting...\n", cause);
 
    g_connected_itmojun = MQTT_DISCONNECTED;
}


static void onConnectCallCBack(void *context, char *cause)
{
    printf("Successful onConnectCallCBack\n");

    g_connected_itmojun = MQTT_CONNECTED;


	MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
	int rc;

	opts.onSuccess = onSubscribe;
	opts.onFailure = onSubscribeFailure;
	opts.context = context;

    // 订阅控制命令主题，从而可以接收到来自移动端的控制命令
	if ((rc = MQTTAsync_subscribe(context, "+/cmd", QOS, &opts)) != MQTTASYNC_SUCCESS)
	{
		printf("Failed to start subscribe, return code %d\n", rc);
	}	
}


static void onDisconnectFailure(void* context, MQTTAsync_failureData* response)
{
    printf("Disconnect failed\n");
 
}


static void onDisconnect(void* context, MQTTAsync_successData* response)
{
    printf("Successful disconnection\n");
 
}


/*
 * MQTT 下行消息回调：当云端/移动端下发控制指令时触发。
 * 逻辑是：
 *   1. 解析 topicName 中的设备 ID
 *   2. 通过哈希表查找设备对应的 socket
 *   3. 调用 send() 把控制命令发送回对应的无线设备
 */
static int messageArrived(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
{
    printf("Message arrived\n");
    printf("topic: %s\n", topicName);
    printf("message(%d): %.*s\n", message->payloadlen, message->payloadlen, (char*)message->payload);

    // 将接收到的消息转发给对应的 ESP8266
    char* p = strchr(topicName, '/');
    char dev_id[31] = "";       /* 初始化，避免异常路径下用脏数据查表 */
    int len;
    long sock_conn = 0;

    if(p != NULL)
    {
	    len = p - topicName;

	    if(len > 0 && len <= 30)
	    {
		    strncpy(dev_id, topicName, len);
		    dev_id[len] = '\0';

		    // 查询哈希表，获取当前设备 ID 对应的连接套接字，并将控制指令发送给对应的设备
		    sock_conn = (long)hash_table_get(ci_table, dev_id);

		    if(sock_conn > 0)
		    {
			    // 如果目标设备在线
			    ssize_t n = send((int)sock_conn, (char*)message->payload, message->payloadlen, MSG_NOSIGNAL);

			    if(n < 0)
			    {
				    printf("转发给设备 %s 失败(%s)，设备可能已离线\n", dev_id, strerror(errno));
			    }
		    }
		    else
		    {
			    printf("设备 %s 不在线，控制命令丢弃\n", dev_id);
		    }
	    }
    }


    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);

    return 1;
}
 


/* 建立一次连接（同步失败不退出，交给看门狗重试） */
static void connect_once(void)
{
    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
    int rc;

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    conn_opts.onSuccess = onConnect;
    conn_opts.onFailure = onConnectFailure;
    conn_opts.context = g_client_itmojun;

    // 断开重连设置
    conn_opts.automaticReconnect = 1;   // 设置非零，断开自动重连
    conn_opts.minRetryInterval = 1;     // 单位秒，重连间隔次数，每次重连失败间隔加倍直到最大值
    conn_opts.maxRetryInterval = 5;     // 单位秒，最大重连尝试间隔

    rc = MQTTAsync_connect(g_client_itmojun, &conn_opts);

    if (rc != MQTTASYNC_SUCCESS && rc != MQTTASYNC_OPERATION_INCOMPLETE)
    {
        fprintf(stderr, "Failed to start connect, return code %d，稍后重试\n", rc);
    }
}


/*
 * FIX #8：Paho 的 automaticReconnect 只在"成功连上过之后断开"才生效。
 *         如果进程比 broker 先启动（或者 broker 当时拒绝连接），
 *         原版只会打印一行 "Connect failed" 然后永远不发数据。
 *         这里加一个只负责"首次连上"的看门狗线程：连上就退出，之后交给 automaticReconnect。
 */
static void *initial_connect_watchdog(void *arg)
{
    (void)arg;

    while(g_connected_itmojun != MQTT_CONNECTED)
    {
        sleep(5);

        if(g_connected_itmojun != MQTT_CONNECTED)
        {
            printf("MQTT 尚未连上，重试中…\n");
            connect_once();
        }
    }

    printf("MQTT 首次连接完成，看门狗线程退出\n");
    return NULL;
}


/*
 * MQTT 客户端初始化：
 * 1. 创建 MQTTAsync 客户端对象
 * 2. 设置回调：连接丢失、消息到达、连接成功
 * 3. 建立连接并订阅 "+/cmd" 主题，准备接收云端控制命令
 */
void mqttclient_comm_init()
{
    int rc;
    pthread_t tid;

	if ((rc = MQTTAsync_create(&g_client_itmojun, ADDRESS_ITMOJUN, CLIENTID_ITMOJUN, MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTASYNC_SUCCESS)
	{
		fprintf(stderr, "Failed to create client object, return code %d\n", rc);
		exit(EXIT_FAILURE);
	}


    //设置连接丢失回调
    if ((rc = MQTTAsync_setCallbacks(g_client_itmojun, g_client_itmojun, connlost, messageArrived, NULL)) != MQTTASYNC_SUCCESS)
    {
        fprintf(stderr, "Failed to set callback, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    //设置连接成功回调
    if ((rc = MQTTAsync_setConnected(g_client_itmojun, g_client_itmojun, onConnectCallCBack)) != MQTTASYNC_SUCCESS)
    {
        fprintf(stderr, "Failed to set callback, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    connect_once();

    if(pthread_create(&tid, NULL, initial_connect_watchdog, NULL) != 0)
    {
        fprintf(stderr, "pthread_create watchdog failed\n");
    }
    else
    {
        pthread_detach(tid);
    }
}
