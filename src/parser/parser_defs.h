#pragma once


int yyparse(void* scanner);

typedef struct yy_buffer_state *YY_BUFFER_STATE;
typedef void* yyscan_t;

// YY_BUFFER_STATE yy_scan_string(const char *str);
YY_BUFFER_STATE yy_scan_string ( const char *yy_str ,yyscan_t yyscanner );

// void yy_delete_buffer(YY_BUFFER_STATE buffer);
void yy_delete_buffer ( YY_BUFFER_STATE b , yyscan_t yyscanner );

int yylex_init (yyscan_t* scanner);
int yylex_destroy ( yyscan_t yyscanner );
