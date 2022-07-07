//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_TODO_H
#define MYSQL_TODO_H

// TODO 这是 customer_type_key 的比较函数，但是 customer_type_key 可能是由多个字段
//  组合成的，这些字段中有一些不是字节类型（如int），是不是需要使用其专用的比较函数
//  在rocksdb中，有专门的Rdb_field_packing类用来将这些特殊字段转变为 mem_comparable 的形式
//  innodb中也有将mysql type转换为 innodb type的转换
//  我感觉彻底解决这个问题需要修改upsDB，让他能支持多个字段的复合键比较
//  upsDB不支持Varchar key和Complex key（多个字段组成的key），我认为这是upsdb的缺陷，
//  所以我的存储引擎目前不支持varchar作为key，也不支持多列key，彻底解决这个问题我认为需
//  要修改upsdb，给他增加这两个功能(增加两个keytype UPS_TYPE_VARCHAR 和 UPS_TYPE_COMPLEX)，
//  其中 UPS_TYPE_VARCHAR 可以直接在创建db的时候在param中指定，UPS_TYPE_COMPLEX 我觉得
//  可以这样处理：首先创建db的时候param中指定{UPS_PARAM_KEY_TYPE, UPS_TYPE_COMPLEX}，
//  然后后面跟着几个 param 均为 {UPS_PARAM_KEY_TYPE, part_type}，表示Complex key的
//  每一个part的实际type，然后在db中再挂一个CustomCompareState的属性用于后面比较
#if 0
// 提取key中的key_part信息，应该是包括key的类型和长度
struct CustomCompareState {
  CustomCompareState(KEY *key) {
    if (key) {
      for (uint32_t i = 0; i < key->user_defined_key_parts; i++) {
        KEY_PART_INFO *key_part = &key->key_part[i];
        parts.emplace_back(key_part->type, key_part->length);
      }
    }
  }

  std::vector<KeyPart> parts;
};

// 自定义的比较函数，按照key_part中的顺序依次比较各个key，按字面值进行比较，可以兼容变长字段
static int custom_compare_func(ups_db_t *db, const uint8_t *lhs,
                               uint32_t lhs_length, const uint8_t *rhs,
                               uint32_t rhs_length) {
  CustomCompareState *ccs = (CustomCompareState *)ups_get_context_data(db, 1);
  CustomCompareState tmp_ccs(
      ccs != nullptr ? nullptr
                     : &recovery_table->key_info[ups_db_get_name(db) - 2]);
  // during recovery, the context pointer was not yet installed. in this case
  // we use the temporary one.
  if (ccs == nullptr) ccs = &tmp_ccs;

  for (size_t i = 0; i < ccs->parts.size(); i++) {
    KeyPart *key_part = &ccs->parts[i];

    int cmp;

    // TODO use *real* comparison function for other columns like uint32, float, ...?
    switch (encoded_length_bytes(key_part->type)) {
      case 0:  // fixed length columns
        cmp = ::memcmp(lhs, rhs, key_part->length);
        lhs_length = key_part->length;
        rhs_length = key_part->length;
        break;
      case 1:
        lhs_length = *lhs;
        lhs += 1;
        rhs_length = *rhs;
        rhs += 1;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      case 2:
        lhs_length = *(uint16_t *)lhs;
        lhs += 2;
        rhs_length = *(uint16_t *)rhs;
        rhs += 2;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      default:
        DBUG_ASSERT(!"shouldn't be here");
    }

    if (cmp != 0) return cmp;
    // cmp == 0
    if (lhs_length < rhs_length) return -1;
    if (lhs_length > rhs_length) return +1;

    // both keys are equal - continue with the next one
    lhs += lhs_length;
    rhs += rhs_length;
  }

  // still here? then both keys must be equal
  return 0;
}
#endif

// TODO upsdb支持的数据类型也太少了吧，修改的时候记得加一个int类型，现在的存储
//  引擎还不能支持整形索引，只能支持非负索引，没有uint都行，怎么能没有int呢

// TODO Catalogue::Table的持久化，create_mysql_table 和 delete_mysql_table这2个
//  函数应该放到env_manager里面，创建Table的时候，把他相关的一些数据用protobuf序列化
//  存到ups的system table中，open table的时候先open env_manager内的Table，找不到
//  再去ups的system table里面找，这个时候Index里面的ken_info也可以赋值了。删除Table
//  需要同时删除ups里面的内容，engine init的时候不需要打开Table。
//  我们不支持online DDL，所以少了很多与Table相关的修改。

#endif  // MYSQL_TODO_H
