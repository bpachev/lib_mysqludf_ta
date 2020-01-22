/*
   Copyright (c) <2020> <Benjamin Pachev>
   Dual licensed under the MIT and GPL licenses.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <mysql.h>
#include <ctype.h>
#include <math.h>
#include "ta_libmysqludf_ta.h"

#define EPSILON .0001 //for prices this is small enough

//An aggregate replica of Bloomberg's Volatility function

/*
   CREATE aggregate FUNCTION ta_volatility_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_volatility_agg;
 */

extern int get_NextSlot(int, int );

typedef struct ta_volatility_agg_data_ {
	int current;
	int next_slot;
	double values[];
} ta_volatility_agg_data;

DLLEXP my_bool ta_volatility_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_volatility_agg_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != 2) {
		strcpy(message, "ta_volatility_agg() requires two arguments");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_volatility_agg() requires a real first argument");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_volatility_agg() requires an integer second argument");
		return 1;
	}

	if (!(data = (ta_volatility_agg_data *)malloc(sizeof(ta_volatility_agg_data) + (*(int *)args->args[1]) * sizeof(double)))) {
		strcpy(message, "ta_volatility_agg() couldn't allocate memory");
		return 1;
	}

	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_volatility_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_volatility_agg_clear(UDF_INIT *initid)
{
	ta_volatility_agg_data *data = (ta_volatility_agg_data *)initid->ptr;
	data->current = 0;
	data->next_slot = 0;
}

DLLEXP void ta_volatility_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_volatility_agg_data *data = (ta_volatility_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];
	if (args->args[0] == NULL) {
		//Just pretend it doesn't exist
		return;
	}

	double val = *((double*)args->args[0]);
	if (val < EPSILON) return; // data problem, price should not be zero or negative. This will cause problems with the log
	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = get_NextSlot(data->next_slot, *periods);	
}

DLLEXP double ta_volatility_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_volatility_agg_data *data = (ta_volatility_agg_data *)initid->ptr;
	int periods = *(int *)args->args[1];


	if (periods > data->current || periods <= 1) {
		*is_null = 1;
		return 0.0;
	} else {
		int i;
		int first = data->next_slot;
		int second;
		double mean_sum = 0.0;
		double square_sum = 0.0;
		for (i = 0; i < periods-1; i++) {
			second = get_NextSlot(first, periods);
			double diff = log(data->values[second])-log(data->values[first]);
			mean_sum += diff;
			square_sum += diff*diff;
			first = second;
		}

		double mean = mean_sum/(periods-1);
		//The sqrt 260 annualizes, and the 100 makes it a percentage
		//Also, Bloomberg appears to be computing sample standard devation, dividing by n-1 instead of n
		return sqrt(square_sum/(periods-1)-mean*mean)*sqrt((periods-1.)/(periods-2.))*sqrt(260.)*100;
	}

}
