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

#define CCI_NUM_ARGS 4
#define CCI_MAGIC_CONSTANT 0.015

/*
   CREATE aggregate FUNCTION ta_cci_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_cci_agg;
 */

typedef struct ta_cci_data_ {
	int current;
	int next_slot;	
	double values[];
} ta_cci_data;

void reset_data(ta_cci_data * data)
{
	data->current = 0;
	data->next_slot = 0;
}

//The true value/price is the average of high, close and low
//It doesn't matter what order the user puts these three numerical arguments in
double get_true_value(UDF_ARGS *args)
{
	double true_value = 0.;
	for (int i = 0; i < CCI_NUM_ARGS-1; i++)
	{
		true_value += *((double*)args->args[i]);
	}
	return true_value / (CCI_NUM_ARGS-1);
}

DLLEXP my_bool ta_cci_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_cci_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != CCI_NUM_ARGS) {
		strcpy(message, "ta_cci_agg() requires four arguments");
		return 1;
	}
	
	for (int i=0; i < CCI_NUM_ARGS-1; i++)
	{
		if (args->arg_type[i] == DECIMAL_RESULT)
			args->arg_type[i] = REAL_RESULT;
		else if (args->arg_type[i] != REAL_RESULT) {
			strcpy(message, "ta_cci_agg() requires that the first three arguments be real");
			return 1;
		}
	}

	if (args->arg_type[3] != INT_RESULT) {
		strcpy(message, "ta_cci_agg() requires an integer");
		return 1;
	}

	if (!(data = (ta_cci_data *)malloc(sizeof(ta_cci_data) + (*(int *)args->args[CCI_NUM_ARGS-1]) * sizeof(double)))) {
		strcpy(message, "ta_cci_agg() couldn't allocate memory");
		return 1;
	}

	reset_data(data);

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_cci_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_cci_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	for (int i = 0; i < CCI_NUM_ARGS; i++) {
		if (args->args[i] == NULL) return;
	}

	ta_cci_data *data = (ta_cci_data *)initid->ptr;
	int *periods = (int *)args->args[CCI_NUM_ARGS-1];
	
	data->current = data->current + 1;
	data->values[data->next_slot] = get_true_value(args);
	data->next_slot = getNextSlot(data->next_slot, *periods);
}

DLLEXP void ta_cci_agg_clear(UDF_INIT *initid)
{
	ta_cci_data *data = (ta_cci_data *)initid->ptr;
	reset_data(data);
}

DLLEXP double ta_cci_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_cci_data *data = (ta_cci_data *)initid->ptr;
	int *periods = (int *)args->args[CCI_NUM_ARGS-1];
	int n = *periods;

	if (n > data->current) {
		*is_null = 1;
		return 0.0;
	} else {		
		double sum = 0.0;
		int i = 0;
		for (i = 0; i < n; i++)
			sum += data->values[i];
		
		double mean = sum / n;
		double value = data->values[(data->current+n-1) % (n)];
		
		sum = 0.0;
		for (i = 0; i < n; i++)
		{
			sum += fabs(data->values[i] - mean);
		}
		double mean_deviation = sum/n;
		return (value - mean) / (CCI_MAGIC_CONSTANT * mean_deviation);
	}
}
