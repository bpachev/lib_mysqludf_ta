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
#include <math.h>
#include "ta_libmysqludf_ta.h"

/*
   CREATE aggregate FUNCTION ta_slope_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_slope_agg;
 */

extern int get_NextSlot(int, int );

typedef struct ta_slope_agg_data_ {
	int current;
	int next_slot;
	double values[];
} ta_slope_agg_data;

DLLEXP my_bool ta_slope_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_slope_agg_data* data;

	initid->maybe_null = 1;

	if (args->arg_count != 2) {
		strcpy(message, "ta_slope_agg() requires two arguments");
		return 1;
	}

	if (args->arg_type[0] == DECIMAL_RESULT)
		args->arg_type[0] = REAL_RESULT;
	else if (args->arg_type[0] != REAL_RESULT) {
		strcpy(message, "ta_slope_agg() requires a real");
		return 1;
	}

	if (args->arg_type[1] != INT_RESULT) {
		strcpy(message, "ta_slope_agg() requires an integer");
		return 1;
	}

	if (!(data = (ta_slope_agg_data *)malloc(sizeof(ta_slope_agg_data) + (*(int *)args->args[1]) * sizeof(double)))) {
		strcpy(message, "ta_slope_agg() couldn't allocate memory");
		return 1;
	}

	data->current = 0;
	data->next_slot = 0;

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_slope_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_slope_agg_clear(UDF_INIT *initid)
{
	ta_slope_agg_data *data = (ta_slope_agg_data *)initid->ptr;
	data->current = 0;
	data->next_slot = 0;
}

DLLEXP void ta_slope_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_slope_agg_data *data = (ta_slope_agg_data *)initid->ptr;
	int *periods = (int *)args->args[1];
	if (args->args[0] == NULL) {
		//Just pretend it doesn't exist
		return;
	}
	data->current = data->current + 1;
	data->values[data->next_slot] = *((double*)args->args[0]);
	data->next_slot = get_NextSlot(data->next_slot, *periods);	
}

DLLEXP double ta_slope_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_slope_agg_data *data = (ta_slope_agg_data *)initid->ptr;
	int n = *(int *)args->args[1];


	if (n > data->current || n <= 1) {
		*is_null = 1;
		return 0.0;
	} else {
		double sum = 0.0;
		double x_dot_y = 0.0;
		int i;
		int slot = data->next_slot;
		for (i = 0; i < n; i++)
		{
			sum += data->values[slot];
			x_dot_y += i*data->values[slot];
			slot = get_NextSlot(slot,n);
		}
		
		//The Slope function returns the slope of a linear regression on the price data
		//The predictor is simply the period number, ranging from 0 to num_periods-1
		//This problem is highly structured, and so it is not necessary to use a least squares library routine
		//I obtained the below formula by writing out the solution to the regression problem in explicit form and then algebriacly reducing it
		//I used the closed forms sum (1 ... n) = n*(n+1)/2
		// and sum (1,4 ... n^2) = n*(n+1)*(2*n+1)/6
		double denom = n*(n+1)/6.;
		return (2./(n-1) * x_dot_y - sum) / denom;
	}
}
