//
// Created by yulan on 22-7-6.
//

#ifndef MYSQL_UTILS_H
#define MYSQL_UTILS_H

#include <ups/upscaledb.h>
#include "sql/log.h"
#include "my_dbug.h"

// helper macros to improve CPU branch prediction
#if defined __GNUC__
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif
typedef std::vector<uint8_t> ByteVector;

void log_error_impl(const char *file, int line, const char *function, ups_status_t st);
#define log_error(f, s) log_error_impl(__FILE__, __LINE__, f, s)

struct KeyPart {
  explicit KeyPart(uint32_t type_ = 0, uint32_t length_ = 0)
      : type(type_), length(length_) {}

  uint32_t type;
  uint32_t length;
};

// A class which aborts a transaction when it's destructed
struct TxnProxy {
  explicit TxnProxy(ups_env_t *env) {
    ups_status_t st = ups_txn_begin(&txn, env, nullptr, nullptr, 0);
    if (unlikely(st != 0)) {
      log_error("ups_txn_begin", st);
      txn = nullptr;
    }
  }

  ~TxnProxy() {
    if (txn != nullptr) {
      ups_status_t st = ups_txn_abort(txn, 0);
      if (unlikely(st != 0)) log_error("ups_txn_abort", st);
    }
  }

  ups_status_t commit() {
    ups_status_t st = ups_txn_commit(txn, 0);
    if (likely(st == 0)) txn = nullptr;
    return st;
  }

  ups_status_t abort() {
    if (txn != nullptr) {
      ups_status_t st = ups_txn_abort(txn, 0);
      if (likely(st == 0)) txn = nullptr;
      return st;
    }
    return 0;
  }

  ups_txn_t *txn;
};

// A class which closes a cursor when going out of scope
struct CursorProxy {
  explicit CursorProxy(ups_cursor_t *c = nullptr) : cursor(c) {}

  explicit CursorProxy(ups_db_t *db, ups_txn_t *txn = nullptr) {
    ups_status_t st = ups_cursor_create(&cursor, db, txn, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_create", st);
      cursor = nullptr;
    }
  }

  ~CursorProxy() {
    if (cursor != nullptr) {
      ups_status_t st = ups_cursor_close(cursor);
      if (unlikely(st != 0)) log_error("ups_cursor_close", st);
    }
  }

  ups_cursor_t *detach() {
    ups_cursor_t *c = cursor;
    cursor = nullptr;
    return c;
  }

  ups_cursor_t *cursor;
};

#endif  // MYSQL_UTILS_H
