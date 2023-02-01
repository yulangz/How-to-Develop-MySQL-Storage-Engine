//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_TODO_H
#define MYSQL_TODO_H

// TODO 多列索引可以直接将全部列转换成 memcomparable 的格式，然后拼接到一起。
//  具体的，整形需要有符号和大小端的转换，浮点型可以参考 myRocks里面的方法（我也把他应用到了MiniOB里面了，也可以参考），
//  varchar将后面补0到完整长度，date转为整形

// TODO upsdb支持的数据类型也太少了吧，修改的时候记得加一个int类型，现在的存储
//  引擎还不能支持整形索引，只能支持非负索引，没有uint都行，怎么能没有int呢

// TODO 已知bug：
//  在 update t set id=id+10 (其中id是int型主键)的时候，会无线循环，原因是，假设原来有id=1，
//  在rnd_next的时候读到了它，并且通过update_row更新了它(id=11)，下一次rnd_next的时候就会又读到更新
//  的值(id=11)，因此造成无限循环

// TODO 不能支持 null 排序
#if 0
struct KeyPart {
  explicit KeyPart(uint32_t type_ = 0, uint32_t length_ = 0)
      : type(type_), length(length_) {}

  uint32_t type;
  uint32_t length;
};

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



#endif  // MYSQL_TODO_H
