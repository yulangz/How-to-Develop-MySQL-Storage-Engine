//
// Created by yulan on 22-6-28.
//

#ifndef MYSQL_CONFIGURATION_H
#define MYSQL_CONFIGURATION_H

#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>

#include <ups/upscaledb_int.h>

#include "catalogue.h"

typedef std::pair<bool, std::string> ParserStatus;

extern ParserStatus
parse_comment_list(const char *comment, Catalogue::Table *cattbl);

extern ParserStatus
parse_config_file(const std::string &filename, Catalogue::Database *catdb,
                  bool is_open);

extern void
write_configuration_settings(std::string env_name, const char *comment,
                             Catalogue::Database *catdb);

#endif  // MYSQL_CONFIGURATION_H
