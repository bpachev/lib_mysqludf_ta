#if defined(_WIN32) || defined(_WIN64)
#define DLLEXP __declspec(dllexport)
#else
#define DLLEXP
#endif

#define MAX_TA_ID_LEN 256

enum ta_id_col_type {NO_ID, INT_ID, STRING_ID, TA_ID_INIT_ERROR};

typedef struct ta_id_info_ {
	enum ta_id_col_type type;
	union {
		struct {
			char * str;
			unsigned long len;
		};
		long long id;
	};
} ta_id_info;

int ta_compare_id(UDF_ARGS* args, int argnum, ta_id_info * curr_id);
ta_id_info init_ta_id(UDF_ARGS* args, int argnum);
void deinit_ta_id(ta_id_info * id);
