#include <sys/types.h>

#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "thr_lock.h"    /* THR_LOCK, THR_LOCK_DATA */

#include "catalogue.h"
#include "key_process.h"
#include "record_pack.h"
#include "utils.h"

/** @brief
  a class that will be shared among all open handlers.
*/
class Sar_share : public Handler_share {
 public:
  THR_LOCK lock;
  Sar_share() { thr_lock_init(&lock); };
  ~Sar_share() override { thr_lock_delete(&lock); }
};

class Sar_Trx {
 public:
  ups_txn_t *tx = nullptr;
  const ulonglong trx_id = 0;
  bool is_registered = false;
  int32_t table_in_use = 0; // 帮助实现 auto commit
};

/** @brief
  SAR storage engine
*/
class ha_sar : public handler {
  // THR_LOCK_DATA lock;      ///< MySQL lock
  Sar_share *share;        ///< Shared lock info
  Sar_share *get_share();  ///< Get the share

  // 当前正在使用的Table
  std::shared_ptr<Catalogue::Table> current_table;
  // 现在正在使用的cursor
  ups_cursor_t *current_cursor;
  bool is_first_after_rnd_init;  // 解决 rnd_init 后第一次读的问题
  Sar_Trx sar_trx;

  // MySQL table lock，按照innodb里面的实现，这个是可以不需要的，暂时保留
  THR_LOCK_DATA lock_data;

  // 缓存key以及row的buffer，避免频繁的构造与析构
  ByteVector key_arena;
  ByteVector record_arena;
  ByteVector old_rec;
  ByteVector old_pk;
  ByteVector old_second_k;
  ByteVector new_second_k;

 public:
  ha_sar(handlerton *hton, TABLE_SHARE *table_arg)
      : handler(hton, table_arg),
        current_table(nullptr),
        current_cursor(nullptr) {
    active_index = MAX_KEY;
  }
  ~ha_sar() override = default;

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override { return "SAR"; }

  /**
    存储引擎的默认索引类型
  */
  enum ha_key_alg get_default_index_algorithm() const override {
    return HA_KEY_ALG_BTREE;
  }
  /**
   * 存储引擎支持的索引，目前只支持B+树
   */
  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override {
    return key_alg == HA_KEY_ALG_BTREE;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override {
    return  // HA_NO_TRANSACTIONS |      //
            // HA_NO_AUTO_INCREMENT |  //
        // HA_REQUIRE_PRIMARY_KEY |
        HA_STATS_RECORDS_IS_EXACT |
        HA_MULTI_VALUED_KEY_SUPPORT |
        HA_PRIMARY_KEY_IN_READ_INDEX |
        HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
        HA_NULL_IN_KEY |
        HA_PRIMARY_KEY_REQUIRED_FOR_DELETE |
        HA_TABLE_SCAN_ON_INDEX |
        HA_HAS_OWN_BINLOGGING |
        HA_BINLOG_FLAGS | // 这个还要以后研究研究
        HA_NO_READ_LOCAL_LOCK |
        HA_GENERATED_COLUMNS;
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx MY_ATTRIBUTE((unused)),
                    uint part MY_ATTRIBUTE((unused)),
                    bool all_parts MY_ATTRIBUTE((unused))) const override {
    return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE
        // TODO　HA_DO_INDEX_COND_PUSHDOWN　待支持
        ;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override {
    return HA_MAX_REC_LENGTH;
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_keys() const override { return MAX_KEY; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_key_parts() const override { return MAX_REF_PARTS; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override {
    return std::numeric_limits<uint16_t>::max();
  }
  Cost_estimate table_scan_cost() override;
  Cost_estimate index_scan_cost(uint index, double ranges,
                                double rows) override;
  Cost_estimate read_cost(uint index, double ranges, double rows) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override;  ///< required
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override;  ///< required
  int close() override;                           ///< required
  int write_row(uchar *buf) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  int index_init(uint idx, bool sorted) override;
  int index_end() override;
  //  int index_read(uchar *buf, const uchar *key, uint key_len,
  //                         enum ha_rkey_function find_flag) override;
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;
  int index_operation(uchar *buf, uint32_t flags);
  int index_next(uchar *buf) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;
  int rnd_init(bool scan) override;              ///< required
  int rnd_end() override;                        /// <required
  int rnd_next(uchar *buf) override;             ///< required
  int rnd_pos(uchar *buf, uchar *pos) override;  ///< required
  void position(const uchar *record) override;   ///< required
  int info(uint) override;                       ///< required
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override;  ///< required
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;
  int delete_table(const char *from, const dd::Table *table_def) override;
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override;
  //  uint lock_count() const;
  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) override;  ///< required
  int analyze(THD *, HA_CHECK_OPT *) override;

  void update_trx(THD *thd);
  int table_primary_key() {return table->s->primary_key == MAX_KEY ? 0 : table->s->primary_key; }
};
