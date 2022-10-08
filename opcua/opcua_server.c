/*  File name:   opcua_server.c
 *  Version:     open62541-1.1.6
 *  Author:      Top-iot
 *  Data:        2022/09/20 
 *  Description: UA_ENABLE_AMALGAMATION & UA_ENABLE_HISTORIZING	*/

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <general.h>
#include <system.h>
#include <target.h>
#include <stdint.h>
#include <util.h>
#include <uci.h>
#include <network.h>
#include <sys/socket.h>
#include <curl/curl.h>
#include <json-c/json.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include "mqtt.h"
#include "db_sqlite3.h"
#include "hj212.h"
#include "config.h"
#include "connect.h"
#include "com.h"
#include <netinet/tcp.h>
#include "open62541.h"

extern int is_sending;  
extern int is_collected;
extern struct factor_t	*factor_cache;
extern struct factor_list_t factor_list;
extern struct signal_inform_t signal_inform;
UA_Boolean running = true;
UA_UInt32 monid = 0;	/* 监控变量序号*/

#define opc_dev_num 4	/* 采集设备数*/
#define control_num 6	/* 反控设备数*/
#define UA_STRINGBUFF_STATIC(CHARS) {strlen(CHARS), (UA_Byte*)CHARS}

void stopHandler(int sign)
{
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
	running = false;
}

/* 添加上报变量*/
void manuallyDefineVariant(struct opcua_server_config * myopcua,  UA_VariableAttributes *factor, 
						UA_NodeId * parentNodeId, UA_NodeId * factor_nodeId)
{
	int i;
	const size_t factor_num = factor_list.total_num;	
	char tmpbuf[128];
	UA_String factor_name[factor_num];		
	UA_String factor_value[factor_num];

	UA_Server *server = myopcua->server;
	UA_HistoryDataGathering gathering = myopcua->gathering;
	UA_HistorizingNodeIdSettings setting = myopcua->setting;

	/* 设置对象*/
	char *devname[opc_dev_num] = {"Dct", "Modbus", "Adc", "Di"};
	UA_ObjectAttributes dev[opc_dev_num];
	for (i = 0; i < opc_dev_num; i++) {
		dev[i] = UA_ObjectAttributes_default;
		dev[i].displayName = UA_LOCALIZEDTEXT("en-US", devname[i]);
		if (i == 0) { 			
			/* dct设备结点*/
			UA_Server_addObjectNode(server, UA_NODEID_NULL,
					UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
					UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
					UA_QUALIFIEDNAME(0, devname[i]),
					UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE), dev[i], NULL, &parentNodeId[i]);
		} else {	 
			/* modbus adc di 设备结点*/
			UA_Server_addObjectNode(server, UA_NODEID_NULL, parentNodeId[0],
					UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
					UA_QUALIFIEDNAME(0, devname[i]),
					UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE), dev[i], NULL, &parentNodeId[i]);
		}
	}

	for (i = 0; i < factor_list.total_num; i++) {
		
		/* 获取变量值*/
		factor_name[i] = UA_STRING(factor_cache[i].factor_name);
		if (factor_cache[i].read_error == 1) {
			factor_value[i] = UA_STRING("Error");
		} else if (factor_cache[i].read_empty == 1) {
			factor_value[i] = UA_STRING("Empty");
		} else {
			sprintf(tmpbuf,"%.3f",factor_cache[i].value);
			factor_value[i] = UA_STRING(tmpbuf);
		}

		/* 设置变量属性*/
		factor[i] = UA_VariableAttributes_default;
		UA_Variant_setScalar(&factor[i].value, &factor_value[i], &UA_TYPES[UA_TYPES_STRING]);
		factor[i].displayName = UA_LOCALIZEDTEXT("en-US", factor_name[i].data);
		factor[i].description = UA_LOCALIZEDTEXT("en-US", factor_name[i].data);
		factor[i].dataType = UA_TYPES[UA_TYPES_STRING].typeId;
		factor[i].accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_HISTORYREAD;
		factor[i].historizing = true;
		
		/* 设置节点*/
		factor_nodeId[i] = UA_NODEID_STRING(0, factor_name[i].data);
		UA_QualifiedName browseName = UA_QUALIFIEDNAME(i, factor_name[i].data);
		UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT);

		/* 添加节点*/
		if (factor_cache[i].belong_dev == 1) { 
			UA_Server_addVariableNode(server, factor_nodeId[i], 
							parentNodeId[1], parentReferenceNodeId,
							browseName,  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE), 
							factor[i], NULL, NULL);
		} else if (factor_cache[i].belong_dev == 2) {	
			UA_Server_addVariableNode(server, factor_nodeId[i],
                            parentNodeId[2], parentReferenceNodeId,
							browseName,  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
							factor[i], NULL, NULL);

		} else if (factor_cache[i].belong_dev == 3) {	
			UA_Server_addVariableNode(server, factor_nodeId[i],
							parentNodeId[3], parentReferenceNodeId,
							browseName,  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
							factor[i], NULL, NULL);
		}
		
		gathering.registerNodeId(server, gathering.context, &factor_nodeId[i], setting);

		DEBUG_INFO("OPCUA Server add: factor[%d].displayName is[%s], factor[%d].description is[%s],factor[%d].value is [%s]", i,factor[i].displayName.text.data, i, factor[i].description.text.data, i, factor_value[i].data );
	}
}

/* 写入上报变量*/
void manuallyWriteVariant(UA_Server * server, UA_VariableAttributes *factor,
                         UA_NodeId * parentNodeId, UA_NodeId * factor_nodeId)
{
	int i;
    const size_t factor_num = factor_list.total_num;
    char tmpbuf[128];
	UA_String factor_name[factor_num];
	UA_String factor_value[factor_num];
	
	for (i = 0; i < factor_list.total_num; i++) {
 	    factor_name[i] = UA_STRING(factor_cache[i].factor_name);
		if (factor_cache[i].read_error == 1) {
            factor_value[i] = UA_STRING("Error");
		} else if (factor_cache[i].read_empty == 1) {
			factor_value[i] = UA_STRING("Empty");
		} else {
			sprintf(tmpbuf,"%.3f",factor_cache[i].value);
	    	factor_value[i] = UA_STRING(tmpbuf);
		}
		UA_Variant_setScalar(&factor[i].value, &factor_value[i], &UA_TYPES[UA_TYPES_STRING]);
		UA_Server_writeValue(server, factor_nodeId[i], factor[i].value);

		DEBUG_INFO("OPCUA Server write: factor[%d].displayName is[%s], factor[%d].description is[%s],factor[%d].value is [%s]", i,factor[i].displayName.text.data, i, factor[i].description.text.data, i, factor_value[i].data);
	}	
}

/* 随上报周期上报采集因子*/
void *set_real_variable_main(void *arg)
{
	opcua_server_config *myopcua = (opcua_server_config *)arg;
	UA_Server *server = myopcua->server;

	const size_t factor_num = factor_list.total_num; 
	UA_VariableAttributes factor[factor_num];		/* 因子变量属性*/
	UA_NodeId factor_nodeId[factor_num];			/* 因子变量结点*/
	UA_NodeId dev_nodeId[opc_dev_num];		        /* 采集设备结点---adc，modbus, adc, di*/
	bool key = true;

	while(TRUE) {
		sem_wait(&signal_inform.sem_arrive);
		if (is_sending == TRUE || is_collected == FALSE) {
			DEBUG_INFO("pthread is sending packet or no data is collected");
			continue;
		}
		DEBUG_INFO("start to set real opcua variable, num is [%d]", factor_num);
		
		is_sending = TRUE;

		if (key == true) {
			/* 添加变量*/
			manuallyDefineVariant(myopcua, factor, dev_nodeId, factor_nodeId);
		} else {
			/* 写入变量*/
			manuallyWriteVariant(server, factor, dev_nodeId, factor_nodeId);
		}
		key = false;
		is_sending = FALSE;
	}
}

void pthread_set_variable(struct opcua_server_config *myopcua)
{
	int ret;
	pthread_t tid;
	
	ret = pthread_create(&tid, NULL, set_real_variable_main, (void*)myopcua);
	if (ret != 0) {
		DEBUG_WARN("Fail to create set real variable thread");
		pthread_exit(NULL);	
	}
											
	pthread_detach(tid);
}

/* 设置服务端名*/
UA_StatusCode UA_ServerConfig_setServerName(UA_ServerConfig *config)
{
	UA_LocalizedText_deleteMembers(&config->applicationDescription.applicationName);
	const char * OPCUA_SERVER_NAME = "Area intelligent controller";
	config->applicationDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en", OPCUA_SERVER_NAME);

	for (size_t i = 0; i < config->endpointsSize; ++i) {
		UA_LocalizedText * ptr = &config->endpoints[i].server.applicationName;
		UA_LocalizedText_deleteMembers(ptr);
		(*ptr) = UA_LOCALIZEDTEXT_ALLOC("en", OPCUA_SERVER_NAME);
	}
	return UA_STATUSCODE_GOOD;
}

/* 设置用户名密码*/
UA_StatusCode set_username_password(UA_ServerConfig *config)
{
	unsigned char username[512] = {0};
	unsigned char password[512] = {0};
	strcpy(username, dct_conf.opcua_server.opcua_username);
	strcpy(password, dct_conf.opcua_server.opcua_password);

	const size_t usernamePasswordsSize = 2;
	UA_UsernamePasswordLogin logins[2] = {
	    {UA_STRING_STATIC("jixun"), UA_STRING_STATIC("jixun")},
		{UA_STRINGBUFF_STATIC(username), UA_STRINGBUFF_STATIC(password)}
	};
	
	DEBUG_INFO("User1: username[%s],password[%s]; User2: username[%s],password[%s]",
	            logins[0].username.data, logins[0].password.data,
				logins[1].username.data, logins[1].password.data);
	
	UA_StatusCode retval = UA_AccessControl_default(config, false,
		        &config->securityPolicies[config->securityPoliciesSize-1].policyUri, 
				usernamePasswordsSize, logins);

	return retval;
}

/* 设置监控对象*/
UA_NodeId UA_Add_AnswerObject(UA_Server *server)
{
	UA_NodeId theAnswerObjectNodeId = UA_NODEID_STRING(1,"Answer");
	UA_ObjectAttributes answer = UA_ObjectAttributes_default;
	answer.displayName = UA_LOCALIZEDTEXT("en-US","Answer");
	UA_Server_addObjectNode(server, theAnswerObjectNodeId,
				UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
				UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
				UA_QUALIFIEDNAME(0, "Answer"),
				UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE), 
				answer, NULL, &theAnswerObjectNodeId);
	
	DEBUG_INFO("AnswerObjectNodeId is [%hd]", theAnswerObjectNodeId.namespaceIndex);
	return theAnswerObjectNodeId;
}

/* 设置监控变量*/
UA_NodeId UA_Add_AnswerVariable(UA_Server *server, UA_NodeId theAnswerObjectNodeId, 
								UA_NodeId theAnswerNodeId, const char *answerName, int index)
{

	UA_VariableAttributes attr = UA_VariableAttributes_default;
    UA_String myInteger = UA_STRING("-1");
    UA_Variant_setScalar(&attr.value, &myInteger, &UA_TYPES[UA_TYPES_STRING]);
	attr.description = UA_LOCALIZEDTEXT("en-US", (char *)answerName);
	attr.displayName = UA_LOCALIZEDTEXT("en-US", (char *)answerName);
	attr.dataType = UA_TYPES[UA_TYPES_STRING].typeId;
	attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;

	theAnswerNodeId = UA_NODEID_NUMERIC(1, index);	/* start: index = 1*/
    UA_QualifiedName myIntegerName = UA_QUALIFIEDNAME(1, (char *)answerName);
	UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
	UA_Server_addVariableNode(server, theAnswerNodeId, theAnswerObjectNodeId,
                             parentReferenceNodeId, myIntegerName,									
							 UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE), 
							 attr, NULL, NULL);
	
	DEBUG_INFO("AnswerNodeId is [%hd]", theAnswerNodeId.namespaceIndex);
	return theAnswerNodeId;

}

/* 监测变量回调函数*/
	/* 返回变量结点index：nodeId->identifier.numeric
	 * 返回数值:         value->value.data
	 * 注意事项：在服务器创建开始会自动调用一轮*/
void dataChangeNotificationCallback(UA_Server *server, UA_UInt32 monitoredItemId,
                               void *monitoredItemContext, const UA_NodeId *nodeId,
							   void *nodeContext, UA_UInt32 attributeId,
							   const UA_DataValue *value)
{
	UA_NodeId * targetNodeId = (UA_NodeId*)monitoredItemContext;
	UA_Int32 targetId = targetNodeId->identifier.numeric;
	UA_Int32 NodeId = nodeId->identifier.numeric;
	UA_Int32 *currentValue = *(UA_Int32 *)(value->value.data);

    UA_String valueStr = *(UA_String*)value->value.data;

	if (NodeId) {
		DEBUG_INFO("get AnswerVariable changed, targetNodeIdname is[%d]; nodeidname is [%d], nodeContextname is [%s], Current Value: [%s]", targetId, NodeId, (char*)nodeContext, valueStr.data);
	}
}

/* 设置监测属性*/
UA_UInt32 addMonitoredItemToVariable(UA_Server *server, UA_NodeId *pTargetNodeId)
{
	UA_MonitoredItemCreateResult result;
	UA_MonitoredItemCreateRequest monRequest = UA_MonitoredItemCreateRequest_default(*pTargetNodeId);
	
	/* 监测间隔 ms*/
	monRequest.requestedParameters.samplingInterval = 100.0; 
	result = UA_Server_createDataChangeMonitoredItem(server, UA_TIMESTAMPSTORETURN_BOTH,		                                            monRequest, (void*)pTargetNodeId, 
									dataChangeNotificationCallback);

	if (result.statusCode == UA_STATUSCODE_GOOD) {
		DEBUG_INFO("Add monitored item for variable, OK");
		return result.monitoredItemId;
	} else {
		DEBUG_INFO("Add monitored item for variable, Fail");
		return -1;
	}
}

/* 设置多个监测变量*/
	/* 监控变量数目: control_num （只能是常量,在开头#define）
	 * 监控变量名称: answerName[ ]*/
UA_StatusCode UA_set_ClientControl(opcua_server_config *myopcua)
{
	UA_Server *server = myopcua->server;
	UA_StatusCode retval;
	UA_NodeId answerNodeId[control_num];
	UA_NodeId answerObjectNodeId;
	char *answerName[control_num] = {"Remote", "Auto", "#1_RD", "#2_RD", "#1_ON", "#2_ON"};
	
	/* 添加监测对象*/
	answerObjectNodeId = UA_Add_AnswerObject(server);

	/* 添加监测结点*/
	for (int i = control_num - 1; i >= 0; i--) {
		answerNodeId[i] = UA_Add_AnswerVariable(server, answerObjectNodeId, 
											answerNodeId[i], answerName[i], i+1);
		monid = addMonitoredItemToVariable(server, &answerNodeId[i]);
		if (monid != -1) {
	        DEBUG_INFO("OPCUA Server Monitored item id: %d", monid);
		} else {
			DEBUG_INFO("OPCUA Server set answer Monitored error");
		}
	}
}

/* 方法回调函数*/
UA_StatusCode ControlMethodCallback(UA_Server *server,
                         			const UA_NodeId *sessionId, void *sessionHandle,
									const UA_NodeId *methodId, void *methodContext,
									const UA_NodeId *objectId, void *objectContext,
									size_t inputSize, const UA_Variant *input,															   size_t outputSize, UA_Variant *output) 		
{
	UA_String *inputStr = (UA_String*)input->data;
	UA_String tmp = UA_STRING_ALLOC("input control cmd success");
	UA_Variant_setScalarCopy(output, &tmp, &UA_TYPES[UA_TYPES_STRING]);

	DEBUG_INFO("controlMethod call [%s]", inputStr->data);
	UA_String_clear(&tmp);

	return UA_STATUSCODE_GOOD;
}

/* 方法属性---客户端交互窗口配置*/
void addControlMethod(UA_Server *server) 
{
	UA_Argument inputArgument;
	UA_Argument_init(&inputArgument);
	inputArgument.description = UA_LOCALIZEDTEXT("en-US", "A String");
	inputArgument.name = UA_STRING("MyInput");
	inputArgument.dataType = UA_TYPES[UA_TYPES_STRING].typeId;
	inputArgument.valueRank = UA_VALUERANK_SCALAR;

	UA_Argument outputArgument;
	UA_Argument_init(&outputArgument);
	outputArgument.description = UA_LOCALIZEDTEXT("en-US", "A String");
	outputArgument.name = UA_STRING("MyOutput");
	outputArgument.dataType = UA_TYPES[UA_TYPES_STRING].typeId;
	outputArgument.valueRank = UA_VALUERANK_SCALAR;

	UA_MethodAttributes controlAttr = UA_MethodAttributes_default;
	controlAttr.description = UA_LOCALIZEDTEXT("en-US","Set Control cmd messages");
	controlAttr.displayName = UA_LOCALIZEDTEXT("en-US","Control Function");
	controlAttr.executable = true;
	controlAttr.userExecutable = true;

	UA_Server_addMethodNode(server, UA_NODEID_NUMERIC(1,62541),
	               UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
				   UA_NODEID_NUMERIC(0, UA_NS0ID_HASORDEREDCOMPONENT),
				   UA_QUALIFIEDNAME(1, "control_dev_name"),
				   controlAttr, &ControlMethodCallback,
				   1, &inputArgument, 1, &outputArgument, NULL, NULL);
}

/* 服务端main*/
void *opcua_server_main()
{
	DEBUG_INFO("OPCUA Server main statring...");
	
	int ret = 0;
	int svr = 0;
	int opcua_port = dct_conf.opcua_server.opcua_server_port;
	opcua_server_config myopcua;
	UA_StatusCode retval;

	signal(SIGINT, stopHandler);
	signal(SIGTERM, stopHandler);

	UA_Server *server = UA_Server_new();
	if (server == NULL) {
		DEBUG_INFO("OPCUA Server create error, return...");
		return;
	} else {
		DEBUG_INFO("OPCUA Server create success");
	}
	
	/* 配置opcua server 默认配置*/
	UA_ServerConfig *config = UA_Server_getConfig(server);
    if (config == NULL) {
		DEBUG_INFO("OPCUA Server create config error, return...");
		UA_Server_delete(server);
		return;
	}

	/* 自定义端口号*/
	retval = UA_ServerConfig_setMinimal(config, opcua_port, NULL);
	if (retval != UA_STATUSCODE_GOOD) {
		DEBUG_INFO("OPCUA Server set port error");
	}else {
		DEBUG_INFO("OPCUA Server set port success");
	}

    /* 自定义服务端名*/
	retval = UA_ServerConfig_setServerName(config);
	if (retval != UA_STATUSCODE_GOOD) {
		DEBUG_INFO("OPCUA Server name anonymous");
	} else {
		DEBUG_INFO("OPCUA Server name set success");
	}

	/* 登录方式---配置用户名密码*/
	if (dct_conf.opcua_server.anonymous == 0) {
		retval = set_username_password(config);
		if (retval != UA_STATUSCODE_GOOD) {
			DEBUG_INFO("OPCUA Server login set error, login by anonymous");
		} else {
			DEBUG_INFO("OPCUA Server login set success, login by username password");
		}
	} else {
		DEBUG_INFO("OPCUA Server login by anonymous");
	}

	/* 数据库插件---历史数据功能*/
	UA_HistoryDataGathering gathering = UA_HistoryDataGathering_Default(1);	
	config->historyDatabase = UA_HistoryDatabase_default(gathering);
	UA_HistorizingNodeIdSettings setting;
	setting.historizingBackend = UA_HistoryDataBackend_Memory(3, 100);
	setting.maxHistoryDataResponseSize = 100;
	setting.historizingUpdateStrategy = UA_HISTORIZINGUPDATESTRATEGY_VALUESET;

	/* 添加只读变量---上报功能*/
	myopcua.server = server;
	myopcua.config = config;
	myopcua.gathering = gathering;
	myopcua.setting = setting;
	pthread_set_variable(&myopcua);

	/* 添加多个监测变量--反控多个设备*/
	retval = UA_set_ClientControl(&myopcua);
	if (retval != UA_STATUSCODE_GOOD) {
		DEBUG_INFO("OPCUA Server set Answer variables error");
	} else {
		DEBUG_INFO("OPCUA Server set Answer variables success");
	}

	/* 添加控制方法*/
	addControlMethod(server);

	/* 运行opcua server, 断开时返回状态码*/
	retval = UA_Server_run(server, &running);
	if (retval != UA_STATUSCODE_GOOD) {
		char * codename = UA_StatusCode_name(retval);
		DEBUG_INFO("OPCUA Server run error :[%s]", codename);
		UA_Server_delete(server);
		DEBUG_INFO("OPCUA Server exit...");
		return;
	} else {
		DEBUG_INFO("OPCUA Server run success");
	}

	UA_Server_delete(server);
	DEBUG_INFO("OPCUA Server exit...");
}

void pthread_opcua_server()
{
	int ret;
	pthread_t tid;
	
	if (dct_conf.opcua_server.enabled == 1) {
		ret = pthread_create(&tid, NULL, opcua_server_main, NULL);
		if (ret != 0) {
			DEBUG_INFO("Fails to create opcua server thread");
			return;
		}
		pthread_detach(tid);
	} else {
		DEBUG_INFO("OPCUA Server disabled, return");
		return;
	}
}

