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
   CREATE aggregate FUNCTION ta_rsi_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_rsi_agg;
 */

struct ta_rsi_agg_data {
	int current;
	double avg_gain;
	double avg_loss;
	double previous_close;
};


DLLEXP my_bool ta_rsi_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	struct ta_rsi_agg_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != 2) {
		strcpy(message, "ta_rsi_agg() requires two arguments");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_rsi_agg() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_rsi_agg() requires an integer");
		return 1;
	}

	if (!(data = (struct ta_rsi_agg_data*)malloc(sizeof(struct ta_rsi_agg_data)))) {
		strcpy(message, "ta_rsi_agg() couldn't allocate memory");
		return 1;
	}

	data->avg_gain = 0.0;
	data->avg_loss = 0.0;
	data->current = 0;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_rsi_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_rsi_agg_clear(UDF_INIT *initid)
{
	struct ta_rsi_agg_data *data = (struct ta_rsi_agg_data *)initid->ptr;
	data->avg_gain = 0.0;
	data->avg_loss = 0.0;
	data->current = 0;	
}

//The add function only does state updating, and the ta_rsi_agg function returns the result with no state updating

DLLEXP void ta_rsi_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	struct ta_rsi_agg_data *data = (struct ta_rsi_agg_data *)initid->ptr;
	double *value = (double *)args->args[0];
	int *periods = (int *)args->args[1];
	double currentGain = 0, currentLoss = 0;

	if (args->args[0] == NULL) {
		return;
	}

	if (data->current == 0) {
		data->current = 1;
		data->previous_close = *value;
		*is_null = 1;
		return;
	}

	data->current = data->current + 1;

	if ((*periods) + 1 >= data->current) {
		if (*value > data->previous_close)
			data->avg_gain += *value - data->previous_close;
		else
			data->avg_loss += data->previous_close - *value;

		data->previous_close = *value;
	} else {
		if (*value > data->previous_close)
			currentGain = *value - data->previous_close;
		else
			currentLoss = data->previous_close - *value;

		if ((*periods) + 2 == data->current) {
			data->avg_gain = (data->avg_gain) / *periods;
			data->avg_loss = (data->avg_loss) / *periods;
		}
		data->avg_gain = (data->avg_gain * (*periods - 1) + currentGain ) / *periods;
		data->avg_loss = (data->avg_loss * (*periods - 1) + currentLoss ) / *periods;
		data->previous_close = *value;
	}
}

DLLEXP double ta_rsi_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	struct ta_rsi_agg_data *data = (struct ta_rsi_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if ((*periods) + 1 >= data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		return 100 - 100 / ( 1 + (data->avg_gain / data->avg_loss));
	}
}
