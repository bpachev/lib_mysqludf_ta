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

#define ADX_NUM_ARGS 3

/*
   CREATE aggregate FUNCTION ta_adx_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_adx_agg;
 */

typedef struct ta_adx_data_ {
	int current;
	double last_high;
	double last_low;
	double d_plus_sum; //The +DM running smoothed average
	double d_minus_sum; //The -DM running smoothed average
	double adx_sum; //The running smoothed average of \d_plus_sum-d_minus_sum|/|d_plus_sum+d_minus_sum|
} ta_adx_data;


void reset_adx_data(ta_adx_data * data)
{
	data->current = 0;
	data->last_high = 0.0;
	data->last_low = 0.0;
	data->d_plus_sum = 0;
	data->d_minus_sum = 0;
	data->adx_sum = 0;
}

DLLEXP my_bool ta_adx_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_adx_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != ADX_NUM_ARGS) {
		strcpy(message, "Usage ta_adx_agg(high, low, num_periods)");
		return 1;
	}
	
	for (int i=0; i < ADX_NUM_ARGS-1; i++)
	{
		if (args->arg_type[i] == DECIMAL_RESULT)
			args->arg_type[i] = REAL_RESULT;
		else if (args->arg_type[i] != REAL_RESULT) {
			strcpy(message, "ta_adx_agg() requires that the first three arguments be real");
			return 1;
		}
	}

	if (args->arg_type[ADX_NUM_ARGS-1] != INT_RESULT) {
		strcpy(message, "The last argument must be a positive number of periods");
		return 1;
	}

	if (!(data = (ta_adx_data *)malloc(sizeof(ta_adx_data)))) {
		strcpy(message, "ta_adx_agg() couldn't allocate memory");
		return 1;
	}

	reset_adx_data(data);

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_adx_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_adx_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_adx_data *data = (ta_adx_data *)initid->ptr;

	for (int i=0; i < ADX_NUM_ARGS; i++) {
		if (args->args[i] == NULL) {
			return;
		}
	}

	int periods = *(int *)args->args[ADX_NUM_ARGS-1];
	double high = *(double*)(args->args[0]);	
	double low = *(double*)(args->args[1]);
	data->current = data->current + 1;

	if (data->current > 1)
	{
		double d_plus = high-data->last_high;
		double d_minus = data->last_low - low;
		if (d_plus < 0) d_plus = 0;
		if (d_minus < 0) d_minus = 0;
		if (d_plus > d_minus) d_minus = 0;
		else if (d_plus == d_minus) {
			d_plus = 0;
			d_minus = 0;
		}
		else if (d_minus > d_plus) d_plus = 0;

		if (data->current <= periods) {
			data->d_plus_sum += d_plus;
			data->d_minus_sum += d_minus;
		}
		else {
			data->d_plus_sum += d_plus - data->d_plus_sum/periods;
			data->d_minus_sum += d_minus - data->d_minus_sum/periods;
		}

	}
	if (data->current >= periods) {
		double dx;
		double denom = data->d_plus_sum+data->d_minus_sum;
		if (denom == 0) dx = 1;
		else dx = fabs(data->d_plus_sum-data->d_minus_sum) / denom;
		if (data->current <= 2*periods-1) {
			data->adx_sum = data->adx_sum + dx;
			if (data->current == 2*periods-1) {
				data->adx_sum /= periods;
			}
		}
		else data->adx_sum += (dx - data->adx_sum)/periods;
	}

	data->last_high = high;
	data->last_low = low;
}

DLLEXP void ta_adx_agg_clear(UDF_INIT *initid)
{
	ta_adx_data *data = (ta_adx_data *)initid->ptr;
	reset_adx_data(data);
}

DLLEXP double ta_adx_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_adx_data *data = (ta_adx_data *)initid->ptr;
	int *periods = (int *)args->args[ADX_NUM_ARGS-1];
	int n = *periods;

	if (2*n-1 > data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		return 	100*(data->adx_sum);	
	}
}
