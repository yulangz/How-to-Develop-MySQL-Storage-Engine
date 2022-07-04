//
// Created by yulan on 22-6-28.
//
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "catalogue.h"

namespace Catalogue {

boost::mutex databases_mutex;
std::map<std::string, Database *> databases;

bool
Table::add_config_value(std::string &key, std::string &value, bool is_open)
{
  if (key == "enable_compression") {
    if (is_open == true)
      return true;
    if (value == "none")
      return true;
    if (value == "zlib") {
      record_compression = UPS_COMPRESSOR_ZLIB;
      return true;
    }
    if (value == "snappy") {
      record_compression = UPS_COMPRESSOR_SNAPPY;
      return true;
    }
    if (value == "lzf") {
      record_compression = UPS_COMPRESSOR_LZF;
      return true;
    }
    return false;
  }

  return false;
}

bool
Database::add_config_value(std::string &key, std::string &value, bool is_open)
{
  if (key == "enable_crc32") {
    if (value == "true") {
      flags |= UPS_ENABLE_CRC32;
      return true;
    }
    if (value == "false")
      return true;
    return false;
  }

  if (key == "disable_recovery") {
    if (value == "true") {
      flags |= UPS_DISABLE_RECOVERY;
      return true;
    }
    if (value == "false")
      return true;
    return false;
  }

  if (key == "enable_server") {
    if (value == "true") {
      is_server_enabled = true;
      return true;
    }
    if (value == "false") {
      is_server_enabled = false;
      return true;
    }
    return false;
  }

  if (key == "server_port") {
    try {
      server_port = boost::lexical_cast<uint16_t>(value);
      return true;
    }
    catch (boost::bad_lexical_cast &) {
      return false;
    }
  }

  if (key == "cache_size") {
    if (value == "unlimited") {
      flags |= UPS_CACHE_UNLIMITED;
      return true;
    }
    try {
      ups_parameter_t p = {UPS_PARAM_CACHE_SIZE,
                           boost::lexical_cast<uint32_t>(value)};
      params.push_back(p);
      return true;
    }
    catch (boost::bad_lexical_cast &) {
      return false;
    }
  }

  if (key == "page_size") {
    if (is_open)
      return true;

    try {
      ups_parameter_t p = {UPS_PARAM_PAGE_SIZE,
                           boost::lexical_cast<uint32_t>(value)};
      params.push_back(p);
      return true;
    }
    catch (boost::bad_lexical_cast &) {
      return false;
    }
  }

  if (key == "file_size_limit") {
    try {
      ups_parameter_t p = {UPS_PARAM_FILE_SIZE_LIMIT,
                           boost::lexical_cast<uint32_t>(value)};
      params.push_back(p);
      return true;
    }
    catch (boost::bad_lexical_cast &) {
      return false;
    }
  }

  // still here? Check if this setting is for one of the tables ($table.key)
  size_t pos = key.find(".");
  if (pos != std::string::npos) {
    std::string table_name(&key[0], &key[pos]);
    key = key.substr(pos + 1);

    // Create a table object if it does not yet exist
    Table *cattbl = tables[table_name];
    if (!cattbl) {
      cattbl = new Table(table_name);
      tables[table_name] = cattbl;
    }

    return cattbl->add_config_value(key, value, is_open);
  }

  return false;
}

void
Database::finalize_config()
{
  // no cache size specified? then set the default (128 mb)
  bool found = false;
  for (std::vector<ups_parameter_t>::iterator it = params.begin();
       it != params.end(); ++it) {
    if (it->name == UPS_PARAM_CACHE_SIZE) {
      found = true;
      break;
    }
  }
  if (!found) {
    ups_parameter_t p = {UPS_PARAM_CACHE_SIZE, kDefaultCacheSize};
    params.push_back(p);
  }

  if (!(flags & UPS_DISABLE_RECOVERY))
    flags |= UPS_AUTO_RECOVERY;

  // add the "terminating" element
  ups_parameter_t p = {0, 0};
  params.push_back(p);
}

} // namespace Catalogue