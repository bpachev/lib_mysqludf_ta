/*
   Copyright (c) <2010> <João Costa>
   Dual licensed under the MIT and GPL licenses.
   Extended <2020> by <Benjamin Pachev>
   An aggregate MySQL udf to compute the percentage of consecutive pairs in a time series that increase
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
   CREATE aggregate FUNCTION ta_pn_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_pn_agg;
 */

typedef struct ta_pn_agg_data_ {
	int current;
	int next_slot;
	double values[];
} ta_pn_agg_data;

DLLEXP my_bool ta_pn_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_pn_agg_data* data;

	initid->maybe_null = 1;

	if ((args->arg_count != 2)) {
		strcpy(message, "Usage ta_pn_agg(value,num_periods)");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_pn_agg() requires a real for the first argument");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_pn_agg() requires an integer for the second argument");
		return 1;
	}

	if (!(data = (ta_pn_agg_data *)malloc(sizeof(ta_pn_agg_data) + (*(int *)args->args[1]+1) * sizeof(double)))) {
		strcpy(message, "ta_pn_agg() couldn't allocate memory");
		return 1;
	}

	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_pn_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_pn_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_pn_agg_data *data = (ta_pn_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];

	if (args->args[0] == NULL) {
		return;
	}

	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = getNextSlot(data->next_slot, *periods+1);
}

DLLEXP void ta_pn_agg_clear(UDF_INIT *initid)
{
	ta_pn_agg_data *data = (ta_pn_agg_data *)initid->ptr;
	data->current = 0;
	data->next_slot = 0;
}

DLLEXP double ta_pn_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_pn_agg_data *data = (ta_pn_agg_data *)initid->ptr;
	int periods = *(int*)args->args[1];

	if (periods >= data->current) {
		*is_null = 1;
		return 0.0;
	} else {
		double sum = 0.0;
		int i = 0;
		for (i = 0; i < periods; i++) sum += (data->values[i+1] > data->values[i]);
		int c = data->next_slot;
		//We are dealing with a circular buffer, so it is possible that the next value sequentially was actually not the next value logically
		//In this case, we need to subtract out an incorrect term and add a term that accounts for the last and first elements
		if (c) {
			sum += (data->values[0]>data->values[periods]) - (data->values[c] > data->values[c-1]);
		}
		return 100*sum / periods;
	}
}
