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

#define DMI_NUM_ARGS 5
#define _DMI_MAX(a,b) ((a>b) ? a : b)
#define _DMI_MIN(a,b) ((b>a) ? a : b)

/*
   CREATE aggregate FUNCTION ta_dmi_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_dmi_agg;
 */

enum dmi_type{PLUS, MINUS,S};

typedef struct ta_dmi_data_ {
	int current;
	double last_high;
	double last_low;
	double last_close;
	double d_plus_sum; //The +DM running smoothed average
	double d_minus_sum; //The -DM running smoothed average
	double tr_sum; //The TR (true range) running smoothed average
	enum dmi_type type;
} ta_dmi_data;


void reset_dmi_data(ta_dmi_data * data)
{
	data->current = 0;
	data->last_high = 0.0;
	data->last_low = 0.0;
	data->last_close = 0;
	data->d_plus_sum = 0;
	data->d_minus_sum = 0;
	data->tr_sum = 0;
}

DLLEXP my_bool ta_dmi_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_dmi_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != DMI_NUM_ARGS) {
		strcpy(message, "Usage ta_dmi_agg(close, high, low, plus_or_minus, num_periods)");
		return 1;
	}
	
	for (int i=0; i < DMI_NUM_ARGS-2; i++)
	{
		if (args->arg_type[i] == DECIMAL_RESULT)
			args->arg_type[i] = REAL_RESULT;
		else if (args->arg_type[i] != REAL_RESULT) {
			strcpy(message, "ta_dmi_agg() requires that the first three arguments be real");
			return 1;
		}
	}

	if ((args->arg_type[3] != INT_RESULT)||(args->arg_type[4] != INT_RESULT)) {
		strcpy(message, "The third and fourth arugments must be integers");
		return 1;
	}

	if (!(data = (ta_dmi_data *)malloc(sizeof(ta_dmi_data)))) {
		strcpy(message, "ta_dmi_agg() couldn't allocate memory");
		return 1;
	}

	reset_dmi_data(data);
	int input_type = *(int*)(args->args[3]);
	if (input_type > 0) data->type = PLUS;
	else if (input_type < 0) data->type = MINUS;
	else data->type = S;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_dmi_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_dmi_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_dmi_data *data = (ta_dmi_data *)initid->ptr;

	for (int i=0; i < DMI_NUM_ARGS; i++) {
		if (args->args[i] == NULL) {
			return;
		}
	}

	int periods = *(int *)args->args[DMI_NUM_ARGS-1];
	double close = *(double*)(args->args[0]);
	double high = *(double*)(args->args[1]);	
	double low = *(double*)(args->args[2]);

	if (data->current > 0)
	{
		double d_plus = high-data->last_high;
		double d_minus = data->last_low - low;
		if (d_plus < 0) d_plus = 0;
		if (d_minus < 0) d_minus = 0;
		if (d_plus > d_minus) {
			d_minus = 0;
		}
		else d_plus = 0;
		double tr = _DMI_MAX(high, data->last_close) - _DMI_MIN(low, data->last_close);

		if (data->current <= periods) {
			data->d_plus_sum += d_plus;
			data->d_minus_sum += d_minus;
			data->tr_sum += tr;
			if (data->current == periods) {
				data->d_plus_sum /= periods;
				data->d_minus_sum /= periods;
				data->tr_sum /= periods;
			}
		}
		else {
			data->d_plus_sum += (d_plus-data->d_plus_sum)/periods;
			data->d_minus_sum += (d_minus-data->d_minus_sum)/periods;
			data->tr_sum += (tr-data->tr_sum)/periods;
		}
	}

	data->current = data->current + 1;
	data->last_close = close;
	data->last_high = high;
	data->last_low = low;
}

DLLEXP void ta_dmi_agg_clear(UDF_INIT *initid)
{
	ta_dmi_data *data = (ta_dmi_data *)initid->ptr;
	reset_dmi_data(data);
}

DLLEXP double ta_dmi_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_dmi_data *data = (ta_dmi_data *)initid->ptr;
	int *periods = (int *)args->args[DMI_NUM_ARGS-1];
	int n = *periods;

	if (n >= data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		//TODO:: Figure out what Otso means by DMIS
		return 	(data->type == PLUS) ? 100*data->d_plus_sum/data->tr_sum : 100*data->d_minus_sum/data->tr_sum;	
	}
}
