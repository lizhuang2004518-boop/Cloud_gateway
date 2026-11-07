/*
 * device_server.c —— 设备接入服务器（ESP8266 裸 TCP，默认 8866）
 *
 * 本文件相对原版的改动（都标了 FIX）：
 *   FIX #1：dev_id 初始化为空串，避免"连上但没发有效数据就断开"时用栈上垃圾做哈希 key
 *   FIX #2：断开时只删除"仍属于本连接"的映射，避免重连后被旧线程删掉新连接
 *   FIX #4：启动时打印监听信息并 fflush，配合 main.c 的行缓冲，日志实时可见
 *   FIX #5：区分"主动断开 / 超时离线 / 接收错误"，全部打日志，不再一律静默 break
 *   FIX #6：设备 ID 比较改成"长度 + 内容"严格匹配，避免前缀相同被误判为同一设备
 *
 * 10 秒无数据判离线的机制保留不变（DEVICE_IDLE_TIMEOUT_SEC），这是原有设计意图。
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#include "mqtt_client.h"
#include "device_server.h"
#include "hash_table.h"

#define MAX_ONLINE_DEVICE_CNT 10000

/* 设备多久没发数据就判定离线（与原版一致，10 秒）。若设备上报周期 >= 10 秒，请调到 3 倍周期。 */
#define DEVICE_IDLE_TIMEOUT_SEC 10

/* recv_line 的返回值：负数表示异常，-2 表示"超时且一个字都没收到" */
#define RECV_ERROR   (-1)
#define RECV_TIMEOUT (-2)


typedef struct 
{
	int sock_conn;
	char ip[16];
	unsigned short port;

} client_info;



void* client_communication(void* arg);
int recv_line(int sock, char* buf, int maxlen);


/*
 * 设备服务器初始化：启动 TCP 监听端口，等待终端设备接入。
 * 调用过程：main() -> mqttclient_comm_init() -> device_server_init()
 * 之后每来一个设备连接，都会创建一个线程去处理该设备的消息。
 */
void device_server_init(void)
{
	ci_table = create_hash_table(MAX_ONLINE_DEVICE_CNT);

	if(ci_table == NULL)
	{
		fprintf(stderr, "create_hash_table failed\n");
		exit(EXIT_FAILURE);
	}

	// 第 1 步：创建监听套接字
	int sock_listen = socket(AF_INET, SOCK_STREAM, 0);

	if(sock_listen == -1)
	{
		perror("socket");
		exit(EXIT_FAILURE);
	}

	// 开启地址复用，以便服务器快速重启
	int opt_val = 1;
	setsockopt(sock_listen, SOL_SOCKET, SO_REUSEADDR, &opt_val, sizeof(opt_val));

	// 第 2 步：绑定地址
	struct sockaddr_in myaddr;
	memset(&myaddr, 0, sizeof(myaddr));
	myaddr.sin_family = AF_INET;              // 指定地址家族为 Internet 地址家族
	myaddr.sin_addr.s_addr = INADDR_ANY;      // 绑定本机任意 IP（公网访问由安全组控制）
	myaddr.sin_port = htons(8866);            // 设备接入端口

	if(bind(sock_listen, (struct sockaddr*)&myaddr, sizeof(myaddr)) == -1)
	{
		perror("bind");
		exit(EXIT_FAILURE);
	}

	// 第 3 步：监听
	if(listen(sock_listen, 5) == -1)
	{
		perror("listen");
		exit(EXIT_FAILURE);
	}

	/* FIX #4：启动就打个日志，否则日志空着很难判断程序到底跑没跑起来 */
	printf("设备接入服务器已启动，监听 0.0.0.0:8866（%d 秒无数据判离线）\n", DEVICE_IDLE_TIMEOUT_SEC);
	fflush(stdout);

	// 设置接收超时时间
	struct timeval tv;
	tv.tv_sec = DEVICE_IDLE_TIMEOUT_SEC;      // 多少秒
	tv.tv_usec = 0;                           // 多少微秒

	pthread_t tid;
	struct sockaddr_in client_addr;           // 用于接收客户端地址信息
	socklen_t addr_len = sizeof(client_addr); // 用于接收客户端地址信息的长度
	client_info* ci = NULL;
	int sock_conn;


	while(1)
	{
		addr_len = sizeof(client_addr);

		// 第 4 步：接受客户端连接请求
		sock_conn = accept(sock_listen, (struct sockaddr*)&client_addr, &addr_len);

		if(sock_conn == -1)
		{
			if(errno == EINTR)
			{
				continue;
			}

			perror("accept");
			continue;
		}

		if (setsockopt(sock_conn, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) //设置超收时间
		{
			perror("setsockopt");
		}

		printf("\n客户端(%s:%hu)连接成功！\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
		fflush(stdout);

		// 创建新线程，负责与客户端通信
		ci = malloc(sizeof(client_info));

		if(ci == NULL)
		{
			perror("malloc");
			close(sock_conn);
			continue;
		}

		ci->sock_conn = sock_conn;
		strncpy(ci->ip, inet_ntoa(client_addr.sin_addr), sizeof(ci->ip) - 1);
		ci->ip[sizeof(ci->ip) - 1] = '\0';
		ci->port = ntohs(client_addr.sin_port);

		if(pthread_create(&tid, NULL, client_communication, ci))
		{
			perror("pthread_create");
			close(sock_conn);
			free(ci);
			continue;
		}
	}

	// 第 7 步：关闭监听套接字
	close(sock_listen);
}



/*
 * 与单个设备建立的通信线程。
 * 作用：读取设备上传数据、解析 topic/payload、记录设备与 socket 的映射关系，
 * 然后将消息转发给 MQTT Broker。
 *
 * 典型流程：
 *   recv_line() -> 解析 "topic payload" -> hash_table_insert() -> mqttclient_pub_msg()
 */
void* client_communication(void* arg)
{
	client_info* ci = arg;
	char dev_id[31] = "";   /* FIX #1：必须初始化，否则断开时会把栈垃圾当 key 用 */
	int is_first_online = 1;


	pthread_detach(pthread_self());//将线程设置为分离状态，线程结束后自动释放资源


	// 第 5 步：收发数据
	char msg[1000];
	int ret;

	while(1)
	{
		ret = recv_line(ci->sock_conn, msg, sizeof(msg));

		if(ret == 0)
		{
			/* FIX #5：对端正常关闭 */
			printf("设备(%s:%hu)主动断开连接\n", ci->ip, ci->port);
			break;
		}

		if(ret == RECV_TIMEOUT)
		{
			/* FIX #5：超出 10 秒没收到数据，判定离线 */
			printf("设备(%s:%hu)超过 %d 秒无数据，判定离线\n",
			       ci->ip, ci->port, DEVICE_IDLE_TIMEOUT_SEC);
			break;
		}

		if(ret < 0)
		{
			/* FIX #5：真正的接收错误 */
			int saved_errno = errno;
			printf("设备(%s:%hu)接收出错(%s)，断开连接\n",
			       ci->ip, ci->port, strerror(saved_errno));
			break;
		}

		printf("\n终端设备说：%s\n", msg);

		char* topic = NULL;//保存主题指针
		char* payload = NULL;//保存消息内容

		topic = strtok(msg, " ");// 以空格为分隔符，获取主题
		payload = strtok(NULL, " "); // 获取消息内容

		if(topic == NULL || payload == NULL)
		{
			printf("消息格式不对（应为 \"topic payload\"）：%s\n", msg);
			continue;
		}

		// 获取设备 ID（topic 第一段）
		char* p = strchr(topic, '/');
		int len;

		if(p == NULL)
		{
			printf("topic 缺少 '/'，丢弃：%s\n", topic);
			break;
		}

		len = p - topic;

		if(len <= 0 || len > 30)
		{
			printf("设备 ID 长度非法(%d)，断开\n", len);
			break;
		}

		if(is_first_online)
		{
			strncpy(dev_id, topic, len);
			dev_id[len] = '\0';

			// 将设备 ID 和对应的连接套接字插入哈希表
			hash_table_insert(ci_table, dev_id, (void*)(long)(ci->sock_conn));
			is_first_online = 0;

			printf("设备 %s 上线（socket=%d）\n", dev_id, ci->sock_conn);
			fflush(stdout);
		}
		else
		{
			/* FIX #6：原版用 strncmp(dev_id, topic, len) 比较，前缀相同时会误判。
			 *         这里要求"长度相同且内容相同"。 */
			if(len != (int)strlen(dev_id) || strncmp(dev_id, topic, len) != 0)
			{
				printf("设备 ID 不一致（已登记 %s，本次 %.*s），断开\n",
				       dev_id, len, topic);
				break;
			}
		}

		// 发布消息到 MQTT 服务器
		mqttclient_pub_msg(&g_client_itmojun, topic, payload);
	}


	// 第 6 步：断开连接（关闭连接套接字，后面就不能再和这个连接套接字对应的客户端通信了）
	close(ci->sock_conn);
	printf("客户端(%s:%hu)断开连接！\n", ci->ip, ci->port);

	/* FIX #2：设备重连时，新连接已经把同一个 dev_id 指向新的 socket。
	 *         旧线程退出时若无条件 remove，会把新连接从在线表里删掉，
	 *         表现为"设备在线、上行正常，但下发控制命令永远收不到"。
	 *         所以只在"表里还是我这个 socket"时才删除。 */
	if(!is_first_online && dev_id[0] != '\0')
	{
		long current = (long)hash_table_get(ci_table, dev_id);

		if(current == (long)ci->sock_conn)
		{
			hash_table_remove(ci_table, dev_id);
			printf("设备 %s 已从在线表移除\n", dev_id);
		}
		else
		{
			printf("设备 %s 已由新连接接管，保留在线表条目\n", dev_id);
		}
	}

	fflush(stdout);

	free(ci);

	return NULL;
}



/*
 * 从 socket 中按行读取一段数据。
 * 用于接收设备发来的文本消息，例如："device/123/temp 25.6\n"
 * 读取到 '\n' 结束，保证每次读取一个完整的消息帧。
 *
 * 返回值：
 *   >0  读到的字节数（正常一行，不含 '\n'）
 *    0  对端关闭连接（EOF 且没读到数据）
 *   -1  接收错误，errno 有效
 *   -2  接收超时且一个字节都没读到（RECV_TIMEOUT，用于判断"设备失联"）
 */
int recv_line(int sock, char* buf, int maxlen)
{
	int n, rc;
	char c;

	for(n = 1; n < maxlen; n++)
	{
		if((rc = recv(sock, &c, 1, 0)) == 1)
		{
			if(c == '\n')
				break;

			*buf++ = c;
		}
		else if(rc == 0)
		{
			if(n == 1)
			{
				*buf = '\0';
				return 0; // EOF, no data read
			}
			break;        // EOF, some data was read
		}
		else
		{
			/* FIX #5：把"超时"和"真错误"区分开 */
			if(errno == EINTR)
			{
				continue;
			}

			if(errno == EAGAIN || errno == EWOULDBLOCK)
			{
				if(n == 1)
				{
					*buf = '\0';
					return RECV_TIMEOUT;
				}
				break;    // 已经读了半行，先交出去，别丢数据
			}

			*buf = '\0';
			return RECV_ERROR;
		}
	}

	*buf = '\0';
	return n;
}
