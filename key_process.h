//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_KEY_PROCESS_H
#define MYSQL_KEY_PROCESS_H

#include <ups/upscaledb.h>
#include "sql/field.h"
#include "sql/key.h"
#include "sql/table.h"

#include "catalogue.h"
#include "utils.h"

/**
 * 将 mysql key 转变为 internal 格式
 * @param key_info mysql key info
 * @param key[out] 转换后的 ups key 类型
 * @param size[out] 转换后的 ups key 大小
 * @return 0 成功，1 失败
 */
int mysql_key_info_to_internal_key_info(KEY *key_info, uint32_t &key,
                                        uint32_t &size);

/**
 * 从一行中提取出一个key，转变为 internal 格式，这个函数适用于 TableRecordFormat
 * 格式
 * @param row_buf mysql row record
 * @param mysql_key_info
 * @param mysql_table
 * @param arena 缓存提取出的 key 的空间
 * @return internal key
 */
ups_key_t key_from_row(const uchar *row_buf, KEY *mysql_key_info,
                       TABLE *mysql_table, Catalogue::Table &cattbl,
                       ByteVector &arena);

/**
 * 将 key_buf 中的 key 转换为 internal key，这个函数适用于 KeyTupleFormat 格式
 * @param mysql_key_buf
 * @param mysql_key_part_map
 * @param mysql_key_info
 * @param mysql_table
 * @param arena
 * @param fill_val
 * @param exact_key_length
 * @return
 */
ups_key_t key_from_key_buf(const uchar *mysql_key_buf,
                           key_part_map mysql_key_part_map, KEY *mysql_key_info,
                           TABLE *mysql_table, ByteVector &arena, uint fill_val,
                           uint32 &exact_key_length);
#endif  // MYSQL_KEY_PROCESS_H
