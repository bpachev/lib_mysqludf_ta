/*
   Copyright (c) <2010> <João Costa>
   Dual licensed under the MIT and GPL licenses.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//#include <my_global.h>
//#include <my_sys.h>
#include <mysql.h>
#include <ctype.h>
#include "ta_libmysqludf_ta.h"
#define ID_ARGNUM 2

/*
   CREATE FUNCTION ta_ema RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_ema;
 */

struct ta_ema_data {
	double sum;
	int current;
	double last_value;
	double alpha;
	ta_id_info id_info;
};

DLLEXP my_bool ta_ema_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	struct ta_ema_data* data;

	initid->maybe_null = 1;

	if (args->arg_count < 2) {
		strcpy(message, "Usage ta_ema(value, periods, [id_str])");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_ema() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_ema() requires an integer");
		return 1;
	}

	if (!(data = (struct ta_ema_data*)malloc(sizeof(struct ta_ema_data)))) {
		strcpy(message, "ta_ema() couldn't allocate memory");
		return 1;
	}

	data->id_info = init_ta_id(args, ID_ARGNUM);
	if (data->id_info.type == TA_ID_INIT_ERROR) {
		strcpy(message, "ta_ema() couldn't allocate memory for id initialization");
	}

	data->alpha = 2 / (double)((*(int *)args->args[1]) + 1);
	data->sum = 0;
	data->current = 0;

	initid->ptr = (char*)data;

	return 0;
}

DLLEXP void ta_ema_deinit(UDF_INIT *initid)
{
	deinit_ta_id(&((struct ta_ema_data *)initid->ptr)->id_info);
	free(initid->ptr);
}

DLLEXP double ta_ema(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	struct ta_ema_data *data = (struct ta_ema_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (args->args[0] == NULL) {
		*is_null = 1;
		return 0.0;
	}

	if (data->id_info.type != NO_ID) {
		if (ta_compare_id(args, ID_ARGNUM, &(data->id_info))) {
			data->current = 0; //Reset the counter, as we are processing a new group of IDs
			data->sum = 0.0;
		}		
	}

	data->current = data->current + 1;

	if (*periods > data->current) {
		//This function will be used for smoothing purposes. In the application, we want to return a value even if there aren't enough periods. We default to the average in that case
		data->sum += *((double*)args->args[0]);
		return data->sum/data->current;
	} else {
		if (*periods == data->current)
			data->last_value = (data->sum + *((double*)args->args[0])) / (data->current);
		else
			data->last_value = (data->alpha * ((*(double *)args->args[0]) - data->last_value)) + data->last_value;
		return data->last_value;
	}
}
