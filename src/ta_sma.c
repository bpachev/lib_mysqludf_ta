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
extern int getNextSlot(int, int );

/*
   CREATE FUNCTION ta_sma RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION sma;
 */

typedef struct ta_sma_data_ {
	double sum;
	int current;
	double last_value;
	int next_slot;
	ta_id_info id_info;
	double values[];
} ta_sma_data;

DLLEXP my_bool ta_sma_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_sma_data* data;

	initid->maybe_null = 1;

	if (args->arg_count < 2) {
		strcpy(message, "Usage ta_sma(close, periods, [id_str])");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_sma() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_sma() requires an integer");
		return 1;
	}

	if (!(data = (ta_sma_data *)malloc(sizeof(ta_sma_data) + (*(int *)args->args[1]) * sizeof(double)))) {
		strcpy(message, "ta_sma() couldn't allocate memory");
		return 1;
	}

	//In this case, we allow a psuedo group by - this will not be an aggregate function, but results will be grouped by the id
	//This will only work if all records for the same ID are processed consecutively, in increasing temporal order
	data->id_info = init_ta_id(args, ID_ARGNUM);
	if (data->id_info.type == TA_ID_INIT_ERROR) {
		strcpy(message, "ta_sma() couldn't allocate memory for id initialization");
	}
	data->sum = 0;
	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
/*
    fprintf(stderr, "Init ta_sma %i done\n", (*(int *) args->args[1]));
    fflush(stderr);
 */
	return 0;
}

DLLEXP void ta_sma_deinit(UDF_INIT *initid)
{
	deinit_ta_id(&((ta_sma_data *)initid->ptr)->id_info);
	free(initid->ptr);
}

DLLEXP double ta_sma(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_sma_data *data = (ta_sma_data *)initid->ptr;
	int n = *(int *)args->args[1];

	if (args->args[0] == NULL) {
		*is_null = 1;
		return 0.0;
	}

	//Check for ID change, resetting if needed
	if (data->id_info.type != NO_ID) {
		//Note that ta_compare_id will both compare and update the ID
		if (ta_compare_id(args, ID_ARGNUM, &(data->id_info))) {
			data->current = 0; //Reset the counter, as we are processing a new group of IDs
			data->next_slot = 0;
		}
	}

	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = getNextSlot(data->next_slot, n);

	double sum = 0.0;
	int i = 0;
	int len = (n > data->current) ? data->current : n;
	for (i = 0; i < len; i++) {
		sum += data->values[i];
	}

	return sum / len;
}
