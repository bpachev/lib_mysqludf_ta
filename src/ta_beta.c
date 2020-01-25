/*
   Copyright (c) <2010> <João Costa>
   Dual licensed under the MIT and GPL licenses.
   Extended <2020> by <Benjamin Pachev>
   An aggregate MySQL udf to compute Beta technical indicators
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

extern int getNextSlot(int, int );
extern int get_lastSlot(int, int);
/*
   CREATE aggregate FUNCTION ta_beta_agg RETURNS REAL SONAME 'lib_mysqludf_ta.so';
   DROP FUNCTION ta_beta_agg;
 */

//The struct won't make it easy to have two arrays, so we will have one array of structs
typedef struct beta_vals
{
	double index;
	double stock;
} beta_vals;

enum beta_type {RAW, PLUS, MINUS, ALPHA, ERROR, T, R2};

typedef struct ta_beta_agg_data_ {
	int current;
	int next_slot;
	int periods;
	double prevIndex;
	double prevStock;
	int use_returns;
	enum beta_type type;
	struct beta_vals values[];
} ta_beta_agg_data;

DLLEXP my_bool ta_beta_agg_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	ta_beta_agg_data* data;

	initid->maybe_null = 1;

	if ((args->arg_count != 4) || (args->arg_type[0] != REAL_RESULT) || (args->arg_type[1] != REAL_RESULT) || (args->arg_type[2] != INT_RESULT) || (args->arg_type[3] != INT_RESULT)) {
		strcpy(message, "Usage ta_beta_agg(index_value (real), stock_value (real),num_periods (int),type (int))");
		return 1;
	}

	int type = *(int*)args->args[3];
	int periods = *(int*)args->args[2];
	if (type < RAW || type > R2) {
		strcpy(message, "type must be one of 0 (RAW), 1 (PLUS), 2 (MINUS), 3 (ALPHA), 4 (ERROR), 5 (T) or 6 (R2)");
		return 1;
	}

	if (!(data = (ta_beta_agg_data *)malloc(sizeof(ta_beta_agg_data) + periods * sizeof(struct beta_vals)))) {
		strcpy(message, "ta_beta_agg() couldn't allocate memory");
		return 1;
	}

	data->type = type;
	data->current = 0;
	data->next_slot = 0;
	data->periods = periods;
	data->use_returns = 1; //In the future, this will be initialized depending on type. For now, we will always use the returns instead of the raw prices

	initid->ptr = (char*)data;
	return 0;
}

DLLEXP void ta_beta_agg_deinit(UDF_INIT *initid)
{
	free(initid->ptr);
}

DLLEXP void ta_beta_agg_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_beta_agg_data *data = (ta_beta_agg_data *)initid->ptr;

	if (args->args[0] == NULL || args->args[1] == NULL) {
		return;
	}

	double index = *(double*)args->args[0];
	double stock = *(double*)args->args[1];
	if (index == 0.0 || stock == 0.0) return; //Bad data, because index and stock prices should always be strictly positive

	data->current = data->current + 1;
	if (data->use_returns  && (data->current > 1)) {
		data->values[data->next_slot].index =  (index / data->prevIndex - 1);
		data->values[data->next_slot].stock = (stock / data->prevStock - 1);
	}
	else if (!data->use_returns) //No need to compute day-to-day returns, just use the raw prices
	{
		data->values[data->next_slot].index =  index;
		data->values[data->next_slot].stock = stock;
	}
	data->next_slot = getNextSlot(data->next_slot, data->periods);
	data->prevIndex = index;
	data->prevStock = stock;
}

DLLEXP void ta_beta_agg_clear(UDF_INIT *initid)
{
	ta_beta_agg_data *data = (ta_beta_agg_data *)initid->ptr;
	data->current = 0;
	data->next_slot = 0;
}

int beta_should_skip(ta_beta_agg_data * data, int i)
{
	double cmp = 0.0;

	if (data->use_returns) {
		cmp = data->values[i].index;
	}
	else {
		double curVal = data->values[i].index;
		//There is no previous value to compare with, so we exclude it
		if (i == data->next_slot) return 1;
		double prevVal = data->values[get_lastSlot(i, data->periods)].index;
		cmp = curVal - prevVal;
	}

	//Not sure about the boundary case when the index change was exactly zero here
	if (((data->type == PLUS) && (cmp <= 0)) || ((data->type == MINUS) && (cmp >= 0))) return 1;
	return 0;
}

DLLEXP double ta_beta_agg(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	ta_beta_agg_data *data = (ta_beta_agg_data *)initid->ptr;
	int n = data->periods;

	//require an extra day if we are using the returns
	if (n + data->use_returns > data->current) {
		*is_null = 1;
		return 0.0;
	}

	double stockMean = 0.0;
	double indexMean = 0.0;
	int total = 0; //How many are actually included in the computation
	
	for (int i = 0; i <= n; i++) {
		if (beta_should_skip(data,i)) continue;
		total++;
		indexMean += data->values[i].index;
		stockMean += data->values[i].stock;
	}

	if (total == 0) {
		*is_null = 0;
		return 0.0;
	}

	indexMean /= total;
	stockMean /= total;
	//Computing variance and co variance;
	double indexVar = 0.0;
	double coVar = 0.0;
	double stockVar = 0.0;
	register double devInd;
	register double devStock;
	for (int i = 0; i < n; i++) {
		if (beta_should_skip(data,i)) continue;
		devInd = data->values[i].index-indexMean;
		devStock = data->values[i].stock-stockMean;
		indexVar += devInd*devInd;
		coVar += devInd*devStock;
		stockVar += devStock * devStock;
	}

	//avoid division by zero
	if (indexVar == 0.0) {
		*is_null = 1;
		return 0.0;
	}

	double beta = coVar/indexVar;
	if ((data->type >= RAW) && (data->type <= MINUS)) return beta;

	double alpha = stockMean - indexMean * beta;
	if (data->type == ALPHA) return alpha;

	//All of the following need the sum of squared residuals
	double SSR = 0.0;
	for (int i=0; i < n; i++)
	{
		double temp = (data->values[i].stock-beta*data->values[i].index-alpha);
		SSR += temp*temp;
	}

	if (data->type == R2) return 1. - (SSR/stockVar);

	if (n <= 2) {
		*is_null = 1;
		return 0.0;
	}
	double beta_error = sqrt(SSR/(indexVar*(n-2)));

	switch (data->type)
	{
		case ERROR: return beta_error;
		case T: return beta/beta_error;
		default:
			*is_null = 1;
			return 0.0;	
	}
}
