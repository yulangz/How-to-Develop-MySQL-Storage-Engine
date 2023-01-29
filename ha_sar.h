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
  Sar_share is a class that will be shared among all open handlers.
  This sar implements the minimum of what you will probably need.
*/
class Sar_share : public Handler_share {
 public:
  THR_LOCK lock;
  Sar_share() { thr_lock_init(&lock); };
  ~Sar_share() override { thr_lock_delete(&lock); }
};

/** @brief
  Class definition for the storage engine
*/
class ha_sar : public handler {
  // THR_LOCK_DATA lock;      ///< MySQL lock
  Sar_share *share;        ///< Shared lock info
  Sar_share *get_share();  ///< Get the share

  // 当前正在使用的Table
  boost::shared_ptr<Catalogue::Table> current_table;
  // 现在正在使用的cursor
  ups_cursor_t *current_cursor;

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
  bool is_index_algorithm_supported(
      enum ha_key_alg key_alg) const override {
    return key_alg == HA_KEY_ALG_BTREE;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override {
    return HA_NO_TRANSACTIONS      // TODO add transactions support
           | HA_NO_AUTO_INCREMENT  // TODO add auto increment support
           // | HA_MULTI_VALUED_KEY_SUPPORT  // TODO  do we need multi-valued
           // key support?
           | HA_TABLE_SCAN_ON_INDEX | HA_FAST_KEY_READ |
           HA_REQUIRE_PRIMARY_KEY | HA_PRIMARY_KEY_IN_READ_INDEX |
           HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
           HA_PRIMARY_KEY_REQUIRED_FOR_DELETE | HA_HAS_OWN_BINLOGGING |
           HA_BINLOG_FLAGS | HA_NO_READ_LOCAL_LOCK | HA_GENERATED_COLUMNS;
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
        // TODO HA_ONLY_WHOLE_INDEX ?什么意思
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

  //  TODO 更精确的实现
  //  /** @brief
  //    Called in test_quick_select to determine if indexes should be used.
  //  */
  //  virtual double scan_time() { return (double)(stats.records) / 20.0 + 10; }
  //
  //  virtual double read_time(uint, uint, ha_rows rows) {
  //    return (double)rows / 20.0 + 1;
  //  }

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
  virtual int index_read(uchar *buf, const uchar *key, uint key_len,
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
  int analyze(THD *, HA_CHECK_OPT *) override { return HA_ADMIN_OK; };
};
