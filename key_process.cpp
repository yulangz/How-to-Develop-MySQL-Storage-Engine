//
// Created by yulan on 22-7-8.
//

#include "key_process.h"

int table_field_info(KEY_PART_INFO *key_part, uint32_t &key, uint32_t &size) {
  // field->type() 是 field 信息（真实类型为
  // enum_field_types），表示这个字段对应的实际 MySQL 类型 key_part->type 等于
  // field->key_type()（真实类型为
  // ha_base_keytype），表示这个字段作为索引时的类型
  // 例如，Timestamp类型的字段，type() 为 MYSQL_TYPE_TIMESTAMP，key_type() 为
  // HA_KEYTYPE_ULONG_INT 更多详细信息可以查看 sql/field.h
  auto field = key_part->field;
  size = field->key_length();

  // size = key_part->store_length; key_part->store_length 包含了 null 标志和
  // varchar的长度前缀占用的空间 下面这段是从 mariaDB
  // 里面截取的，可以用来判断一个字段是不是null
  //  if (field->real_maybe_null()) {
  //    DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 1));
  //    if (field->is_real_null()) {
  //      /* NULL value. store '\0' so that it sorts before non-NULL values */
  //      *tuple++ = 0;
  //      /* That's it, don't store anything else */
  //      if (n_null_fields) (*n_null_fields)++;
  //      return tuple;
  //    } else {
  //      /* Not a NULL value. Store '1' */
  //      *tuple++ = 1;
  //    }
  //  }

  switch (key_part->type) {
      // TODO: upsdb里面都是无符号数，这里忽略了这一个问题，
      //  也就是说对负数的支持会出问题
    case HA_KEYTYPE_INT8:
      key = UPS_TYPE_UINT8;
      return 0;
    case HA_KEYTYPE_SHORT_INT:  // TODO: 没有区分有符号数和无符号数
    case HA_KEYTYPE_USHORT_INT:
      key = UPS_TYPE_UINT16;
      return 0;
    case HA_KEYTYPE_LONG_INT:
    case HA_KEYTYPE_ULONG_INT:
      key = UPS_TYPE_UINT32;
      return 0;
    case HA_KEYTYPE_LONGLONG:
    case HA_KEYTYPE_ULONGLONG:
      key = UPS_TYPE_UINT64;
      return 0;
    case HA_KEYTYPE_FLOAT:
      key = UPS_TYPE_REAL32;
      return 0;
    case HA_KEYTYPE_DOUBLE:
      key = UPS_TYPE_REAL64;
      return 0;
    case HA_KEYTYPE_TEXT:
    case HA_KEYTYPE_BINARY:
    case HA_KEYTYPE_NUM:
    case HA_KEYTYPE_BIT:
      // 对于变长字符串
      // 索引里面的key会保存整个varchar的全部长度，所以不需要额外那个长度标识
      // 也就是说字段类型 varchar(5)，数据为 'ab'，在 upsDB 里面存的 key 是
      // 'ab\0\0\0'
    case HA_KEYTYPE_VARTEXT1:
    case HA_KEYTYPE_VARBINARY1:
    case HA_KEYTYPE_VARTEXT2:
    case HA_KEYTYPE_VARBINARY2:
      // upsDB里面没有直接给3字节int的类型，所以必须转换成binary类型存储
    case HA_KEYTYPE_INT24:
    case HA_KEYTYPE_UINT24:
      key = UPS_TYPE_BINARY;
      return 0;
  }
  return 1;
}

int table_key_info(KEY *key_info, uint32_t &key, uint32_t &size,
                   __attribute__((unused))
                   std::vector<std::pair<uint32_t, uint32_t>> *key_parts) {
  // if this index is made from a single column: return type/size of this column
  if (key_info->user_defined_key_parts == 1)
    return table_field_info(key_info->key_part, key, size);

  // Otherwise accumulate the total size of all columns which form this key.
  key = UPS_TYPE_BINARY;
  size = 0;
  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
    uint32_t tp, sz;
    if (table_field_info(&key_info->key_part[i], tp, sz) != 0) {
      return 1;
    }
    size += sz;
  }
  return 0;
}

ups_key_t field_from_row(const uchar *buf, KEY_PART_INFO *key_part) {
  uint16_t key_size = key_part->field->pack_length();
  uint32_t offset = key_part->offset;

  switch (length_bytes_len(key_part->type)) {
    case 1:
      key_size = *(uint8_t *)&buf[offset];
      offset++;
      key_size = std::min(key_size, key_part->length);
      break;
    case 2:
      key_size = *(uint16_t *)&buf[offset];
      offset += 2;
      key_size = std::min(key_size, key_part->length);
      break;
  }

  ups_key_t key;
  if ((key_part->key_part_flag & HA_VAR_LENGTH_PART) == 0) // 存的实际上是个指针，指针指向的地方才是真正的数据
    key = ups_make_key(*(uint8_t **)(buf + offset), key_size);
  else
   key = ups_make_key((uchar *)buf + offset, key_size);
  return key;
}

static void convert_int8(uint8* to, ups_key_t key, bool unsigned_flag) {
  auto ptr = (uchar *)key.data;
  if (unsigned_flag)
   *to= *ptr;
  else
   to[0] = (char) (ptr[0] ^ (uchar) 128);	/* Revers signbit */
}
static void convert_int16(uint8* to, ups_key_t key, bool unsigned_flag) {
  auto ptr = (uchar *)key.data;
  if (unsigned_flag)
   to[0] = ptr[1];
  else
   to[0] = (char) (ptr[1] ^ 128);              /* Revers signbit */
  to[1]   = ptr[0];
}
static void convert_int24(uint8* to, ups_key_t key, bool unsigned_flag) {
  auto ptr = (uchar *)key.data;
  if (unsigned_flag)
   to[0] = ptr[2];
  else
   to[0] = (uchar) (ptr[2] ^ 128);		/* Revers signbit */
  to[1] = ptr[1];
  to[2] = ptr[0];
}
static void convert_int32(uint8* to, ups_key_t key, bool unsigned_flag) {
  auto ptr = (uchar *)key.data;
  if (unsigned_flag)
   to[0] = ptr[3];
  else
   to[0] = (char) (ptr[3] ^ 128);              /* Revers signbit */
  to[1]   = ptr[2];
  to[2]   = ptr[1];
  to[3]   = ptr[0];
}
static void convert_int64(uint8* to, ups_key_t key, bool unsigned_flag) {
  auto ptr = (uchar *)key.data;
  if (unsigned_flag)
   to[0] = ptr[7];
  else
   to[0] = (char) (ptr[7] ^ 128);		/* Revers signbit */
  to[1]   = ptr[6];
  to[2]   = ptr[5];
  to[3]   = ptr[4];
  to[4]   = ptr[3];
  to[5]   = ptr[2];
  to[6]   = ptr[1];
  to[7]   = ptr[0];
}

#define doublestore(T,V) memcpy((T), (void*) &V, sizeof(double))
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define float4get(V,M)   memcpy(&V, (M), sizeof(float))
#define float4store(V,M) memcpy(V, (&M), sizeof(float))
#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))
#define FLT_EXP_DIG (sizeof(float)*8-FLT_MANT_DIG)

static void convert_float32(uint8* to, ups_key_t key) {
  auto ptr = (uchar *)key.data;
  float nr;
  float4get(nr,ptr);

  uchar *tmp= to;
  if (nr == (float) 0.0)
  {						/* Change to zero string */
   tmp[0]=(uchar) 128;
   bzero((char*) tmp+1,sizeof(nr)-1);
  }
  else
  {
#ifdef WORDS_BIGENDIAN
   memcpy(tmp, &nr, sizeof(nr));
#else
   tmp[0]= ptr[3]; tmp[1]=ptr[2]; tmp[2]= ptr[1]; tmp[3]=ptr[0];
#endif
   if (tmp[0] & 128)				/* Negative */
   {						/* make complement */
      uint i;
      for (i=0 ; i < sizeof(nr); i++)
        tmp[i]= (uchar) (tmp[i] ^ (uchar) 255);
   }
   else
   {
      ushort exp_part=(((ushort) tmp[0] << 8) | (ushort) tmp[1] |
                         (ushort) 32768);
      exp_part+= (ushort) 1 << (16-1-FLT_EXP_DIG);
      tmp[0]= (uchar) (exp_part >> 8);
      tmp[1]= (uchar) exp_part;
   }
  }
}
#define DBL_EXP_DIG (sizeof(double)*8-DBL_MANT_DIG)

static void change_double_for_sort(double nr,uchar *to)
{
  uchar *tmp=(uchar*) to;
  if (nr == 0.0)
  {						/* Change to zero string */
   tmp[0]=(uchar) 128;
   memset(tmp+1, 0, sizeof(nr)-1);
  }
  else
  {
#ifdef WORDS_BIGENDIAN
   memcpy(tmp, &nr, sizeof(nr));
#else
   {
      uchar *ptr= (uchar*) &nr;
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
      tmp[0]= ptr[3]; tmp[1]=ptr[2]; tmp[2]= ptr[1]; tmp[3]=ptr[0];
      tmp[4]= ptr[7]; tmp[5]=ptr[6]; tmp[6]= ptr[5]; tmp[7]=ptr[4];
#else
      tmp[0]= ptr[7]; tmp[1]=ptr[6]; tmp[2]= ptr[5]; tmp[3]=ptr[4];
      tmp[4]= ptr[3]; tmp[5]=ptr[2]; tmp[6]= ptr[1]; tmp[7]=ptr[0];
#endif
   }
#endif
   if (tmp[0] & 128)				/* Negative */
   {						/* make complement */
      uint i;
      for (i=0 ; i < sizeof(nr); i++)
        tmp[i]=tmp[i] ^ (uchar) 255;
   }
   else
   {					/* Set high and move exponent one up */
      ushort exp_part=(((ushort) tmp[0] << 8) | (ushort) tmp[1] |
                         (ushort) 32768);
      exp_part+= (ushort) 1 << (16-1-DBL_EXP_DIG);
      tmp[0]= (uchar) (exp_part >> 8);
      tmp[1]= (uchar) exp_part;
   }
  }
}

static void convert_float64(uint8* to, ups_key_t key) {
  auto ptr = (uchar *)key.data;
  double nr;
  float8get(nr,ptr);
  change_double_for_sort(nr, to);
}

static void convert_string(uint8* to, ups_key_t key, uint32_t mem_length) {
  bzero(to, mem_length);
  memcpy(to, key.data, key.size);
}

void convert_field_to_mem(KEY_PART_INFO *key_part, ups_key_t key,
                          ByteVector &arena) {
  uint32_t mem_length = key_part->field->key_length();
  auto old_sz = arena.size();
  arena.resize(old_sz + mem_length);
  uint8* p = &arena[old_sz];
  // 在mariaDB里面，每一个字段都有 sort_string方法，可以直接将它们转换为 memcomparable 格式，
  // mysql 里面没有，以下大部分代码都是从mariaDB里面 copy 过来的
  // TODO 目前只有整形、浮点型、字符串支持有序排列(缺乏字符集支持)，decimal类型排序也许会出问题，如果迁移到 mariaDB 里面，
  //  直接调用 sort_string 方法，就不会出问题了
  switch (key_part->type) {
    case HA_KEYTYPE_INT8:
      convert_int8(p, key, key_part->field->unsigned_flag);
      return;
    case HA_KEYTYPE_SHORT_INT:  // TODO: 没有区分有符号数和无符号数
    case HA_KEYTYPE_USHORT_INT:
      convert_int16(p, key, key_part->field->unsigned_flag);
      return;
    case HA_KEYTYPE_INT24:
    case HA_KEYTYPE_UINT24:
      convert_int24(p, key, key_part->field->unsigned_flag);
      return;
    case HA_KEYTYPE_LONG_INT:
    case HA_KEYTYPE_ULONG_INT:
      convert_int32(p, key, key_part->field->unsigned_flag);
      return;
    case HA_KEYTYPE_LONGLONG:
    case HA_KEYTYPE_ULONGLONG:
      convert_int64(p, key, key_part->field->unsigned_flag);
      return;
    case HA_KEYTYPE_FLOAT:
      convert_float32(p, key);
      return;
    case HA_KEYTYPE_DOUBLE:
      convert_float64(p, key);
      return;
    case HA_KEYTYPE_TEXT:
    case HA_KEYTYPE_BINARY:
    case HA_KEYTYPE_NUM: // TODO 这个（num类型）应该还需要一些特别的转换
    case HA_KEYTYPE_BIT:
      // 对于变长字符串
      // 索引里面的key会保存整个varchar的全部长度，所以不需要额外那个长度标识
      // 也就是说字段类型 varchar(5)，数据为 'ab'，在 upsDB 里面存的 key 是
      // 'ab\0\0\0'
    case HA_KEYTYPE_VARTEXT1:
    case HA_KEYTYPE_VARBINARY1:
    case HA_KEYTYPE_VARTEXT2:
    case HA_KEYTYPE_VARBINARY2:
      convert_string(p, key, mem_length);
      return ;
  }
}

ups_key_t key_from_row(const uchar *buf, KEY *key_info, ByteVector &arena) {
  arena.clear();

  if (likely(key_info->user_defined_key_parts == 1))
    return field_from_row(buf, key_info->key_part);

  // 多列索引
  arena.clear();
  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
    KEY_PART_INFO *key_part = &key_info->key_part[i];
    ups_key_t field_key = field_from_row(buf, key_part);
    convert_field_to_mem(key_part, field_key, arena);
  }

  ups_key_t key = ups_make_key(arena.data(), (uint16_t)arena.size());
  return key;
}
