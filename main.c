#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "mqtt_client.h"
#include "device_server.h"

#define LOG_PATH "/tmp/device_cloud.log"
#define PID_PATH "/tmp/device_cloud.pid"
#define MAX_LOG_SIZE (10 * 1024 * 1024)


static void rotate_log_if_needed(const char *path, size_t max_size)
{
    struct stat st;

    if (stat(path, &st) != 0) {
        return;
    }

    if ((size_t)st.st_size < max_size) {
        return;
    }

    char backup[512];
    snprintf(backup, sizeof(backup), "%s.1", path);

    remove(backup);
    if (rename(path, backup) != 0) {
        perror("rename log");
    }
}


static void daemonize(const char *log_path)
{
    rotate_log_if_needed(log_path, MAX_LOG_SIZE);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(1);
    }

    if (pid > 0) {
        exit(0);
    }

    if (setsid() < 0) {
        perror("setsid");
        exit(1);
    }

    umask(0);

    if (chdir("/") < 0) {
        perror("chdir");
        exit(1);
    }

    int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        perror("open log file");
        exit(1);
    }

    if (dup2(fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        exit(1);
    }

    if (dup2(fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        exit(1);
    }

    if (fd > STDERR_FILENO) {
        close(fd);
    }

    /* FIX #4：stdout 重定向到普通文件后，glibc 默认是全缓冲（4KB），
     *         日志要攒满 4KB 才落盘，tail -f 会长时间一片空白，
     *         很容易让人误判"程序没跑起来"。改成行缓冲，写一行刷一行。 */
    setvbuf(stdout, NULL, _IOLBF, 0);

    FILE *fp = fopen(PID_PATH, "w");
    if (fp != NULL) {
        fprintf(fp, "%d\n", getpid());
        fclose(fp);
    }
}


/*
 * 程序主入口
 * 1. 以守护进程方式后台运行
 * 2. 屏蔽 SIGPIPE，避免 TCP 断开时程序因为写入到已关闭 socket 而异常退出
 * 3. 初始化 MQTT 客户端，建立与 MQTT Broker 的连接并订阅控制主题
 * 4. 启动设备接入服务器，等待无线设备 TCP 连接
 */
int main(void)
{
    daemonize(LOG_PATH);

    // 忽略 SIGPIPE 信号
    signal(SIGPIPE, SIG_IGN);

    // 连接 MQTT 服务器，订阅感兴趣的主题
    mqttclient_comm_init();

    // 初始化设备服务器，接收设备连接
    device_server_init();

    return 0;
}
