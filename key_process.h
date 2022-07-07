//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_KEY_PROCESS_H
#define MYSQL_KEY_PROCESS_H

#include <ups/upscaledb.h>
#include "sql/field.h"
#include "sql/key.h"
#include "sql/table.h"

#include "utils.h"

// 返回一个type类型的field的前缀长度，blob何text类型不应该由此判断
inline int encoded_length_bytes(uint8 type) {
  if (type == HA_KEYTYPE_VARTEXT1 || type == HA_KEYTYPE_VARBINARY1) return 1;
  if (type == HA_KEYTYPE_VARTEXT2 || type == HA_KEYTYPE_VARBINARY2) return 2;
  return 0;
}

// 返回一个key_part(一个field)在upsdb中的类型，以及字节大小
// 对于定长字段，返回的size是其字节大小，对于变长字段，返回的size是0
// return <key, size>
// 返回值：如果是可以做索引的字段，返回1,否则返回0
int table_field_info(KEY_PART_INFO *key_part, uint32_t &key, uint32_t &size);

// 返回一个KEY在upsdb中的类型，以及字节大小，一个KEY可以包含多个key_part(field)，要全部累计起来
// return <key, size>
// 如果index只有1个字段，key是这个字段相应的ups_type，size是对应的字节大小
// 返回值：如果是可以做索引的字段，返回1,否则返回0
// 若果index包含多个字段，会返回复合键类型(Complex)，暂时不支持，返回1
int table_key_info(KEY *key_info, uint32_t &key, uint32_t &size,
                   __attribute__((unused))
                   std::vector<std::pair<uint32_t, uint32_t>> *key_parts);

// 从一个行数据中提取出一个key_part(field)的数据，生成ups_key_t
ups_key_t field_from_row(const uchar *buf, KEY_PART_INFO *key_part);

// buf指向一个行，提取其中第index个KEY(可能包含多个Field)的值，生成ups_key_t
ups_key_t key_from_row(const uchar *buf,  KEY* key_info,
                       ByteVector &arena);

#endif  // MYSQL_KEY_PROCESS_H
