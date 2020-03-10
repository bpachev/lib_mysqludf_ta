#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <mysql.h>
#include <ctype.h>
#include <math.h>
#include "ta_libmysqludf_ta.h"

int ta_strcmp(char * str1, char * str2, unsigned long len1, unsigned long len2);

ta_id_info init_ta_id(UDF_ARGS* args, int argnum)
{
	ta_id_info res;
	if (args->arg_count <= argnum) {
		res.type = NO_ID;
		return res;
	}
	if (args->arg_type[argnum] == INT_RESULT) {
		res.type = INT_ID;
		res.id = -1;
		return res;
	}
	else if (args->arg_type[argnum] == STRING_RESULT) {
		res.type = STRING_ID;
		res.str = (char *)malloc(MAX_TA_ID_LEN * sizeof(char));
		res.len = 0; //Initially the string is empty
		if (!res.str) res.type = TA_ID_INIT_ERROR; //Let the caller know an error occured with the id initialization
		return res;
	}
	else {
		res.type = NO_ID;
		return res;
	}
}

void deinit_ta_id(ta_id_info * id)
{
	if (id->type == STRING_ID) free(id->str);
}

//Compares the ID passed to the caller to the current ID. If it is different, updates the current ID and returns 1. Else, returns 0;
// If an error occurs, returns -1.
int ta_compare_id(UDF_ARGS* args, int argnum, ta_id_info * curr_id)
{
	int cmp;
	long long val;
	if (args->arg_count <= argnum) return -1;
	switch (curr_id->type)
	{
		case INT_ID:
			val = *(long long *)args->args[argnum];
			cmp = (curr_id->id != val);
			if (cmp) curr_id->id = val;
			return cmp;
		case STRING_ID:
			cmp = ta_strcmp(curr_id->str, (char*)args->args[argnum], curr_id->len, (unsigned long)args->lengths[argnum]); //requires the lengths of both strings
			if (cmp) {
				curr_id->len = (unsigned long)args->lengths[argnum];
				memcpy(curr_id->str, (char*)args->args[argnum], curr_id->len);
			}
			return cmp;
		default:
			return -1;
	}
}

int ta_strcmp(char * str1, char * str2, unsigned long len1, unsigned long len2)
{
	if (len1 != len2) return 1;
	return (strncmp(str1, str2, len1)) ? 1 : 0;
}
