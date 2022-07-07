//
// Created by yulan on 22-7-6.
//

#include "utils.h"

void log_error_impl(const char *file, int line, const char *function,
                           ups_status_t st) {
  sql_print_error("%s[%d] %s: failed with error %d (%s)", file, line, function,
                  st, ups_strerror(st));
}
