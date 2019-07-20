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

/*
   CREATE aggregate FUNCTION ta_ema_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_ema_agg;
 */

struct ta_ema_agg_data {
	double sum;
	int current;
	double last_value;
	double alpha;
	int do_p; // if nonzero, then instead of returning the EMA, return the last value processed divided by the ema
	double current_data_value;
};

DLLEXP my_bool ta_ema_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	struct ta_ema_agg_data* data;
	int do_p = 0;

	initid->maybe_null = 1;

	if ((args->arg_count < 2)||(args->arg_count > 3)) {
		strcpy(message, "Usage ta_ema_agg(value,num_periods,do_p (optional))");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "Data column must be real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "Number of periods must be integer and positive");
		return 1;
	}

	if (args->arg_count == 3) {
		if (args->arg_type[2] != INT_RESULT) {
			strcpy(message, "Expected integer for third argument");
			return 1;
		}
		if(*(int*)args->args[2]) do_p = 1;
	}

	if (!(data = (struct ta_ema_agg_data*)malloc(sizeof(struct ta_ema_agg_data)))) {
		strcpy(message, "ta_ema_agg() couldn't allocate memory");
		return 1;
	}

	data->alpha = 2 / (double)((*(int *)args->args[1]) + 1);
	data->sum = 0;
	data->current = 0;
	data->do_p = do_p;
	data->current_data_value=0;
	
	initid->ptr = (char*)data;

	return 0;
}

DLLEXP void ta_ema_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_ema_agg_clear(UDF_INIT *initid)
{
	struct ta_ema_agg_data *data = (struct ta_ema_agg_data *)initid->ptr;
	data->sum = 0;
	data->current = 0;
}

DLLEXP void ta_ema_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	struct ta_ema_agg_data *data = (struct ta_ema_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (args->args[0] == NULL) {
		return;
	}

	data->current = data->current + 1;
	double value = *((double*)args->args[0]);
	data->current_data_value = value;

	if (*periods > data->current) {
		data->sum += value;
	} else {
		if (*periods == data->current)
			data->last_value = (data->sum + value) / (data->current);
		else
			data->last_value = (data->alpha * (value - data->last_value)) + data->last_value;
	}	
}

DLLEXP double ta_ema_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	struct ta_ema_agg_data *data = (struct ta_ema_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (*periods > data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		return (data->do_p) ? data->current_data_value/data->last_value : data->last_value;
	}
}
