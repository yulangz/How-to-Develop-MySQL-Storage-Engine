//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_RECORD_PACK_H
#define MYSQL_RECORD_PACK_H

#include "sql/table.h"
#include "sql/field.h"
#include "utils.h"

#include <ups/upscaledb.h>

// 判断一个Mysql的row长度是不是固定的(是不是不包含变长字段)
inline bool row_is_fixed_length(TABLE *table) {
  // table->s里面保存的是一个Table不变的一些信息
  return table->s->blob_fields + table->s->varchar_fields == 0;
}

// 提取varchar字段的长度信息，保存在len_bytes里面
void extract_varchar_field_info(Field *field, uint32_t *len_bytes,
                                              uint32_t *field_size,
                                              const uint8_t *src);

// 把upsdb的record转换成为Mysql的row，只有Row中包含变长字段的时候才需要，
// 数据会缓存到buf中
ups_record_t unpack_record(TABLE *table, ups_record_t *record,
                                         uint8_t *buf);

// 把Mysql的row转换成为upsdb的record，只有Row中包含变长字段的时候才需要
ups_record_t pack_record(TABLE *table, uint8_t *buf,
                         ByteVector &arena);

// 把Mysql的row转换成为upsdb的record
// 如果表中没有变长字段(varchar和blob、text)，那么返回的record会指向buf，arena不会使用
// 否则，把row按各个字段的实际长度提取出来，暂存于arena，并且返回的record指向arena中的data
inline ups_record_t record_from_row(TABLE *table, uint8_t *buf,
                                           ByteVector &arena) {
  // 参考：https://www.yuque.com/yulan-updh1/dxuyyg/pv8zwg#LaJFu

  // fixed length rows do not need any packing
  if (row_is_fixed_length(table)) {
    return ups_make_record(buf, (uint32_t)table->s->stored_rec_length);
  }

  // but if rows have variable length (i.e. due to a VARCHAR field) we
  // pack them to save space
  return pack_record(table, buf, arena);
}

#endif  // MYSQL_RECORD_PACK_H
