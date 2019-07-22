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

extern int getNextSlot(int, int );

#define WILLR_NUM_ARGS 4

/*
   CREATE aggregate FUNCTION ta_willr_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_willr_agg;
 */

typedef struct willr_row_
{
	double high;
	double low;
} willr_row;

typedef struct ta_willr_data_ {
	int current;
	int next_slot;
	double current_close;
	willr_row values[];
} ta_willr_data;

void reset_willr_data(ta_willr_data * data)
{
	data->current = 0;
	data->next_slot = 0;
	data->current_close = 0;
}

DLLEXP my_bool ta_willr_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_willr_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != WILLR_NUM_ARGS) {
		strcpy(message, "Usage: ta_willr_agg(close, high, low, num_periods)");
		return 1;
	}
	
	for (int i=0; i < WILLR_NUM_ARGS-1; i++)
	{
		if (args->arg_type[i] == DECIMAL_RESULT)
			args->arg_type[i] = REAL_RESULT;
		else if (args->arg_type[i] != REAL_RESULT) {
			strcpy(message, "ta_willr_agg() requires that the first three arguments be real");
			return 1;
		}
	}

	if (args->arg_type[3] != INT_RESULT) {
		strcpy(message, "ta_willr_agg() requires an integer");
		return 1;
	}

	if (!(data = (ta_willr_data *)malloc(sizeof(ta_willr_data) + (*(int *)args->args[WILLR_NUM_ARGS-1]) * sizeof(willr_row)))) {
		strcpy(message, "ta_willr_agg() couldn't allocate memory");
		return 1;
	}

	reset_willr_data(data);

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_willr_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_willr_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	for (int i = 0; i < WILLR_NUM_ARGS; i++) {
		if (args->args[i] == NULL) return;
	}

	ta_willr_data *data = (ta_willr_data *)initid->ptr;
	int *periods = (int *)args->args[WILLR_NUM_ARGS-1];
	
	data->current = data->current + 1;
	data->current_close = *(double*)args->args[0];

	willr_row row;
	row.high = *(double*)(args->args[1]);
	row.low = *(double*)(args->args[2]);

	data->values[data->next_slot] = row;
	data->next_slot = getNextSlot(data->next_slot, *periods);
}

DLLEXP void ta_willr_agg_clear(UDF_INIT *initid)
{
	ta_willr_data *data = (ta_willr_data *)initid->ptr;
	reset_willr_data(data);
}

DLLEXP double ta_willr_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_willr_data *data = (ta_willr_data *)initid->ptr;
	int *periods = (int *)args->args[WILLR_NUM_ARGS-1];
	int n = *periods;

	if (n > data->current) {
		*is_null = 1;
		return 0.0;
	} else {		
		double highest_high = -1000000;
		double lowest_low = -highest_high;
		int i = 0;
		for (i = 0; i < n; i++)
		{
			if (data->values[i].high > highest_high) highest_high = data->values[i].high;
			if (data->values[i].low < lowest_low) lowest_low = data->values[i].low;		
		}

		return -100*(highest_high - data->current_close) / (highest_high-lowest_low);
	}
}
