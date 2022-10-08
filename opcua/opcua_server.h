#include "open62541.h"

struct opcua_server_t{
	int enabled;
	int anonymous;
	int opcua_server_port;
	unsigned char opcua_username[512];
	unsigned char opcua_password[512];
};

typedef struct opcua_server_config{
	UA_Server *server;
	UA_ServerConfig *config;
	UA_HistoryDataGathering gathering;
	UA_HistorizingNodeIdSettings setting;

}opcua_server_config;

static void add_variable(UA_Server *server);

static void stopHandler(int sign);

void *opcua_server_main();

void pthread_opcua_server();



