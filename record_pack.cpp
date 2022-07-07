//
// Created by yulan on 22-7-8.
//

#include "record_pack.h"
ups_record_t pack_record(TABLE *table, uint8_t *buf,
                         ByteVector &arena) {
  DBUG_ASSERT(!row_is_fixed_length(table));

  uint8_t *src = buf;
  arena.clear();
  arena.reserve(1024);
  uint8_t *dst = arena.data();

  // copy the "null bytes" descriptors
  arena.resize(table->s->null_bytes);
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  for (Field **field = table->field; *field != nullptr; field++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + size + len_bytes);
      dst = &arena[pos];

      ::memcpy(dst, src, size + len_bytes);
      src += (*field)->pack_length();
      dst += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_BLOB ||
        type == MYSQL_TYPE_MEDIUM_BLOB || type == MYSQL_TYPE_LONG_BLOB) {
      uint32_t packlength;
      uint32_t length;
      extract_varchar_field_info(*field, &packlength, &length, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + sizeof(uint32_t) + length);
      dst = &arena[pos];

      *(uint32_t *)dst = length;
      dst += sizeof(uint32_t);
      ::memcpy(dst, *(char **)(src + packlength), length);
      dst += length;
      src += packlength + sizeof(void *);
      continue;
    }

    size = (*field)->key_length();

    // make sure we have sufficient space
    uint32_t pos = dst - arena.data();
    arena.resize(arena.size() + size);
    dst = &arena[pos];

    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(arena.data(), (uint32_t)(dst - &arena[0]));
  return r;
}

ups_record_t unpack_record(TABLE *table, ups_record_t *record,
                           uint8_t *buf){
  DBUG_ASSERT(!row_is_fixed_length(table));

  auto *src = (uint8_t *)record->data;
  uint8_t *dst = buf;

  // copy the "null bytes" descriptors
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  uint32_t i = 0;
  for (Field **field = table->field; *field != nullptr; field++, i++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);
      ::memcpy(dst, src, size + len_bytes);
      if (likely((*field)->pack_length() > size + len_bytes))
        ::memset(dst + size + len_bytes, 0,
                 (*field)->pack_length() - (size + len_bytes));
      dst += (*field)->pack_length();
      src += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_BLOB ||
        type == MYSQL_TYPE_MEDIUM_BLOB || type == MYSQL_TYPE_LONG_BLOB) {
      size = *(uint32_t *)src;
      src += sizeof(uint32_t);
      Field_blob *blob = *(Field_blob **)field;
      switch (blob->pack_length() - 8) {
        case 1:
          *dst = (uint8_t)size;
          break;
        case 2:
          *(uint16_t *)dst = (uint16_t)size;
          break;
        case 3:  // TODO not yet tested...
          dst[2] = (size & 0xff0000) >> 16;
          dst[1] = (size & 0xff00) >> 8;
          dst[0] = (size & 0xff);
          break;
        case 4:
          *(uint32_t *)dst = size;
          break;
        case 8:
          *(uint64_t *)dst = size;
          break;
        default:
          DBUG_ASSERT(!"not yet implemented");
          break;
      }

      dst += (*field)->pack_length() - 8;
      *(uint8_t **)dst = src;
      src += size;
      dst += sizeof(uint8_t *);
      continue;
    }

    size = (*field)->pack_length();
    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(buf, (uint32_t)(dst - buf));
  return r;
}

void extract_varchar_field_info(Field *field, uint32_t *len_bytes,
                                uint32_t *field_size,
                                const uint8_t *src){
  // see Field_blob::Field_blob() (in field.h) - need 1-4 bytes to
  // store the real size
  if (likely(field->field_length <= 255)) {
    *field_size = *src;
    *len_bytes = 1;
    return;
  }
  if (likely(field->field_length <= 65535)) {
    *field_size = *(uint16_t *)src;
    *len_bytes = 2;
    return;
  }
  if (likely(field->field_length <= 16777215)) {
    *field_size = (src[2] << 16) | (src[1] << 8) | (src[0] << 0);
    *len_bytes = 3;
    return;
  }
  *field_size = *(uint32_t *)src;
  *len_bytes = 4;
}

