/*
   Copyright (c) <2019> <Benjamin Pachev>
   Dual licensed under the MIT and GPL licenses.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include <mysql.h>
#include <ctype.h>
#include "ta_libmysqludf_ta.h"

#define ATR_NUM_ARGS 4
#define ATR_MAX_ARGS 5
#define _ATR_MAX(a,b) ((a>b) ? a : b)
#define _ATR_MIN(a,b) ((b>a) ? a : b)

/*
   CREATE aggregate FUNCTION ta_atr_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_atr_agg;
 */

enum atr_type{PLUS, MINUS,S};

typedef struct ta_atr_data_ {
	int current;
	double last_close;
	double tr_sum; //The TR (true range) running smoothed average
	int do_p;
} ta_atr_data;


void reset_atr_data(ta_atr_data * data)
{
	data->current = 0;
	data->last_close = 0.0;
	data->tr_sum = 0.0;
}

DLLEXP my_bool ta_atr_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_atr_data* data;

	initid->maybe_null = 1;

	if ((args->arg_count < ATR_NUM_ARGS) || (args->arg_count > ATR_MAX_ARGS)) {
		strcpy(message, "Usage ta_atr_agg(close, high, low,num_periods, do_p (option))");
		return 1;
	}
	
	for (int i=0; i < 3; i++)
	{
		if (args->arg_type[i] == DECIMAL_RESULT)
			args->arg_type[i] = REAL_RESULT;
		else if (args->arg_type[i] != REAL_RESULT) {
			strcpy(message, "ta_atr_agg() requires that the first three arguments be real");
			return 1;
		}
	}

	if ((args->arg_type[ATR_NUM_ARGS-1] != INT_RESULT)) {
		strcpy(message, "The fourth arugment must be an integer");
		return 1;
	}

	if (!(data = (ta_atr_data *)malloc(sizeof(ta_atr_data)))) {
		strcpy(message, "ta_atr_agg() couldn't allocate memory");
		return 1;
	}

	reset_atr_data(data);
	int do_p = 0;
	if (args->arg_count == ATR_MAX_ARGS) do_p=*(int*)(args->args[ATR_MAX_ARGS-1]);
	data->do_p = do_p;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_atr_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_atr_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_atr_data *data = (ta_atr_data *)initid->ptr;

	for (int i=0; i < ATR_NUM_ARGS; i++) {
		if (args->args[i] == NULL) {
			return;
		}
	}

	int periods = *(int *)args->args[ATR_NUM_ARGS-1];
	double close = *(double*)(args->args[0]);
	double high = *(double*)(args->args[1]);	
	double low = *(double*)(args->args[2]);
	data->current = data->current + 1;

	if (data->current > 1)
	{
		double tr = _ATR_MAX(high, data->last_close) - _ATR_MIN(low, data->last_close);

		if (data->current <= periods+1) {
			data->tr_sum += tr;
			if (data->current == periods+1) data->tr_sum /= periods;
		}
		else {
			data->tr_sum += (tr - data->tr_sum)/periods;
		}
	}

	data->last_close = close;
}

DLLEXP void ta_atr_agg_clear(UDF_INIT *initid)
{
	ta_atr_data *data = (ta_atr_data *)initid->ptr;
	reset_atr_data(data);
}

DLLEXP double ta_atr_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_atr_data *data = (ta_atr_data *)initid->ptr;
	int *periods = (int *)args->args[ATR_NUM_ARGS-1];
	int n = *periods;

	if (n >= data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		return 	(data->do_p) ? data->last_close/data->tr_sum : data->tr_sum;	
	}
}
