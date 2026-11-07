#ifndef _MQTT_CLIENT_H_
#define _MQTT_CLIENT_H_


#include "MQTTAsync.h"


extern MQTTAsync g_client_itmojun;

void mqttclient_comm_init();
void mqttclient_pub_msg(MQTTAsync* client, const char* topic, const char* payload);


#endif
