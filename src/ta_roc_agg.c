/*
   Copyright (c) <2012> <Joshua Ostrom>
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
   CREATE aggregate FUNCTION ta_roc_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_roc_agg;
 */

extern int get_NextSlot(int, int );

typedef struct ta_roc_agg_data_ {
	int current;
	int next_slot;
	double values[];
} ta_roc_agg_data;

int get_lastSlot(int current_slot, int arr_size)
{
	if (current_slot == 0) return arr_size-1;
	else return current_slot-1;
}

DLLEXP my_bool ta_roc_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_roc_agg_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != 2) {
		strcpy(message, "ta_roc_agg() requires two arguments");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_roc_agg() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_roc_agg() requires an integer");
		return 1;
	}

	if (!(data = (ta_roc_agg_data *)malloc(sizeof(ta_roc_agg_data) + (1+*(int *)args->args[1]) * sizeof(double)))) {
		strcpy(message, "ta_roc_agg() couldn't allocate memory");
		return 1;
	}

	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_roc_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_roc_agg_clear(UDF_INIT *initid)
{
	ta_roc_agg_data *data = (ta_roc_agg_data *)initid->ptr;
	data->current = 0;
	data->next_slot = 0;
}

DLLEXP void ta_roc_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_roc_agg_data *data = (ta_roc_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];
	if (args->args[0] == NULL) {
		//Just pretend it doesn't exist
		return;
	}
	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = get_NextSlot(data->next_slot, *periods+1);	
}

DLLEXP double ta_roc_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_roc_agg_data *data = (ta_roc_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];


	if (*periods > data->current || *periods < 1) {
		*is_null = 1;
		return 0.0;
	} else {
		int current_slot = get_lastSlot(data->next_slot, *periods+1);
		return 100*(data->values[current_slot]-data->values[data->next_slot])/data->values[data->next_slot];
	}

}
