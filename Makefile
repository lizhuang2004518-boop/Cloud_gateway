# 设备接入服务器
# 依赖：gcc、libpaho-mqtt-dev（提供 MQTTAsync.h 和 libpaho-mqtt3a）
#
#   make        编译出 ds
#   make clean  清理
#   make run    前台跑一次（调试用，会自己 daemon 化）

CC      = gcc
CFLAGS  = -O2 -Wall
LDLIBS  = -lpaho-mqtt3a -lpthread

ds: main.c hash_table.c mqtt_client.c device_server.c
	$(CC) $(CFLAGS) $^ -o ds $(LDLIBS)

clean:
	rm -f ds

run: ds
	./ds

.PHONY: clean run
