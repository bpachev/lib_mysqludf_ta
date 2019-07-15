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

extern int getNextSlot(int, int );

/*
   CREATE FUNCTION ta_sma_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_sma_agg;
 */

typedef struct ta_sma_agg_data_ {
	double sum;
	int current;
	double last_value;
	int next_slot;
	double values[];
} ta_sma_agg_data;

DLLEXP my_bool ta_sma_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_sma_agg_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != 2) {
		strcpy(message, "ta_sma_agg() requires two arguments");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_sma_agg() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_sma_agg_agg() requires an integer");
		return 1;
	}

	if (!(data = (ta_sma_agg_data *)malloc(sizeof(ta_sma_agg_data) + (*(int *)args->args[1]) * sizeof(double)))) {
		strcpy(message, "ta_sma_agg() couldn't allocate memory");
		return 1;
	}

	data->sum = 0;
	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
/*
    fprintf(stderr, "Init ta_sma_agg %i done\n", (*(int *) args->args[1]));
    fflush(stderr);
 */
	return 0;
}

DLLEXP void ta_sma_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_sma_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_sma_agg_data *data = (ta_sma_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (args->args[0] == NULL) {
		return;
	}

	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = getNextSlot(data->next_slot, *periods);
}

DLLEXP void ta_sma_agg_clear(UDF_INIT *initid)
{
	ta_sma_agg_data *data = (ta_sma_agg_data *)initid->ptr;
	data->sum = 0;
	data->current = 0;
	data->next_slot = 0;
}

DLLEXP double ta_sma_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_sma_agg_data *data = (ta_sma_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (*periods > data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		double sum = 0.0;
		int i = 0;
		for (i = 0; i < *periods; i++)
			sum += data->values[i];

		return sum / *periods;
	}
}
