/*
 * Copyright (C) 2011 Mauricio Fernandez <mfp@acm.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version,
 * with the special exception on linking described in file LICENSE.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/comparator.h>
#include "leveldb/cache.h"

extern "C" {

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/callback.h>
#include <caml/custom.h>
#include <caml/signals.h>
#include <string.h>

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define Val_none Val_int(0)

typedef struct ldb_any {
  void *data;
  void (*release)(void *);
  intnat id; // signed int w/ same size as pointer
  bool closed;
  bool *in_use;
  bool auto_finalize;
} ldb_any;


typedef struct { leveldb::DB *db; } ldb_handle;
typedef struct { leveldb::Iterator *it; } ldb_iterator;
typedef struct { leveldb::WriteBatch *batch; } ldb_writebatch;

typedef struct {
    const leveldb::Snapshot *snapshot;
    leveldb::DB *db;
    intnat id;
    bool closed;
    bool *in_use;
} ldb_snapshot;

static void ldb_any_finalize(value t);
static int ldb_any_compare(value t1, value t2);
static intnat ldb_any_hash(value t);

static intnat wrapped_val_id = 1;

#define LDB_ANY(x) ((ldb_any *) Data_custom_val(x))

#define LDB_HANDLE(x) (((ldb_handle *) Data_custom_val(x))->db)
#define LDB_ITERATOR(x) (((ldb_iterator *) Data_custom_val(x))->it)
#define LDB_WRITEBATCH(x) (((ldb_writebatch *) Data_custom_val(x))->batch)

#define UNWRAP_SNAPSHOT(x) ((ldb_snapshot *) Data_custom_val(x))

#define WRAP(_dst, _data, _type, _auto_release) \
  do { \
      leveldb::_type *p = _data; \
      _dst = caml_alloc_custom(&ldb_any_ops, sizeof(ldb_any), 0, 1); \
      LDB_ANY(_dst)->data = p; \
      LDB_ANY(_dst)->closed = false; \
      LDB_ANY(_dst)->id = ++wrapped_val_id; \
      LDB_ANY(_dst)->in_use = (bool *)malloc(sizeof(bool)); \
      *(LDB_ANY(_dst)->in_use) = false; \
      LDB_ANY(_dst)->auto_finalize = _auto_release; \
      LDB_ANY(_dst)->release = (void (*)(void *))release_##_type; \
  } while(0);

static struct custom_operations ldb_any_ops = {
    (char *)"org.eigenclass/leveldb_any",
    ldb_any_finalize,
    ldb_any_compare,
    ldb_any_hash,
    custom_serialize_default,
    custom_deserialize_default
};

static void ldb_snapshot_finalize(value);
static intnat ldb_snapshot_hash_(value);

static struct custom_operations ldb_snapshot_ops =
{
 (char *)"org.eigenclass/leveldb_snapshot",
 ldb_snapshot_finalize,
 ldb_any_compare,
 ldb_snapshot_hash_,
 custom_serialize_default,
 custom_deserialize_default
};

static const value *not_found_exn = 0;
static const value *error_exn = 0;

static void raise_error(const char *s)
{
 if(!error_exn) error_exn = caml_named_value("org.eigenclass/leveldb/Error");
 if(!error_exn)
     caml_failwith(s);
 else
     caml_raise_with_string(*error_exn, s);
}

static void
raise_status_error_and_release(const leveldb::Status &status)
{
 using namespace std;
 CAMLparam0();
 CAMLlocal1(err);
 std::string msg = status.ToString();

 err = caml_copy_string(msg.c_str());

 msg.~string();
 status.~Status();

 if(!error_exn) error_exn = caml_named_value("org.eigenclass/leveldb/Error");
 if(!error_exn)
     caml_failwith("<LevelDB: error message not available>");
 else
     caml_raise_with_arg(*error_exn, err);

 CAMLreturn0;
}

#define RAISE_NOT_FOUND \
    do { \
	if(!not_found_exn) \
	    not_found_exn = caml_named_value("org.eigenclass/leveldb/Not_found"); \
	if(!not_found_exn) \
	    caml_failwith("Not_found"); \
	else \
	    caml_raise_constant(*not_found_exn); \
    } while(0);

#define CHECK_ERROR(status) \
    do { \
      if(!status.ok()) raise_status_error_and_release(status); \
    } while(0);

#define CHECK_ERROR_AND_CLEANUP(status, cleanup) \
    do { \
      if(!status.ok()) { \
          do { cleanup } while(0); \
          raise_status_error_and_release(status); \
      } \
    } while(0);

#define CHECK_CLOSED(t) \
    do { \
      if(LDB_ANY(t)->closed || !LDB_ANY(t)->in_use || !LDB_HANDLE(t)) \
        raise_error("leveldb handle closed"); \
    } while(0);

#define CHECK_IT_CLOSED(_it) \
    do { \
      if(LDB_ANY(_it)->closed || !LDB_ANY(_it)->in_use || !LDB_ITERATOR(_it)) \
        raise_error("iterator closed"); \
    } while(0);

#define CHECK_SNAPSHOT_CLOSED(_s) \
    do { \
      if(UNWRAP_SNAPSHOT(_s)->closed || !UNWRAP_SNAPSHOT(_s)->in_use || \
         !UNWRAP_SNAPSHOT(_s)->snapshot) \
        raise_error("invalid snapshot"); \
    } while(0);

#define USE_HANDLE(t) \
    bool *__resource_in_use__##t = LDB_ANY(t)->in_use; \
    CHECK_CLOSED(t); \
    *__resource_in_use__##t = true;

#define USE_IT(t) \
    bool *__resource_in_use__##t = LDB_ANY(t)->in_use; \
    CHECK_IT_CLOSED(t); \
    *__resource_in_use__##t = true;

#define USE_SNAPSHOT(t) \
    bool *__resource_in_use__##t = UNWRAP_SNAPSHOT(t)->in_use; \
    CHECK_SNAPSHOT_CLOSED(t); \
    *__resource_in_use__##t = true;

#define RELEASE(t) \
    do { \
        *__resource_in_use__##t = false; \
    } while(0);

#define RELEASE_HANDLE(t)  RELEASE(t)

#define RELEASE_IT(t) RELEASE(t)

#define RELEASE_SNAPSHOT(t) RELEASE(t)

#if SIZEOF_PTR < 8

// see http://www.concentric.net/~ttwang/tech/inthash.htm
int32_t hash_int(int32_t key)
{
  // >> is implementation-dependent, we assume it's arithmetic and hope for
  // the best (most compilers will do arithmetic if the int is signed; GCC,
  // for one, does)
  key = ~key + (key << 15); // key = (key << 15) - key - 1;
  key = key ^ (key >> 12);
  key = key + (key << 2);
  key = key ^ (key >> 4);
  key = key * 2057; // key = (key + (key << 3)) + (key << 11);
  key = key ^ (key >> 16);
  return (int32_t)key;
}

#else

int64_t hash_int(int64_t key)
{
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return (int64_t)key;
}
#endif

static void
ldb_any_finalize(value t)
{
 ldb_any *h = LDB_ANY(t);

 h->closed = true;
 // wait until the resource is not being used in another thread
 while(h->in_use && *(h->in_use)) {
     struct timeval tv;
     tv.tv_sec = 0;
     tv.tv_usec = 1000;
     select(0, NULL, NULL, NULL, &tv);
 }
 if(h->auto_finalize) {
     if(h->data) {
         h->release(h->data);
         h->data = NULL;
     }
     if(h->in_use) {
         free(h->in_use);
         h->in_use = NULL;
     }
 }
}

static int
ldb_any_compare(value t1, value t2)
{
 ldb_any *h1, *h2;
 h1 = LDB_ANY(t1);
 h2 = LDB_ANY(t2);
 return ((char*)h1->data - (char *)h2->data);
}

static intnat
ldb_any_hash(value t)
{
 return hash_int(LDB_ANY(t)->id);
}

static intnat
ldb_snapshot_hash_(value t1)
{
  return hash_int(UNWRAP_SNAPSHOT(t1)->id);
}

CAMLprim value
ldb_snapshot_hash(value t1)
{
 return Val_long(ldb_snapshot_hash_(t1));
}

CAMLprim value
ldb_iterator_hash(value t1)
{
 return Val_long(ldb_any_hash(t1));
}

static void release_DB(leveldb::DB *db)
{
 delete db;
}

static void release_Iterator(leveldb::Iterator *iterator)
{
 delete iterator;
}

static void release_WriteBatch(leveldb::WriteBatch *writebatch)
{
 delete writebatch;
}

CAMLprim value
ldb_lexicographic_comparator(value unit)
{
 const leveldb::Comparator *c = leveldb::BytewiseComparator ();

 return (value)c;
}

CAMLprim value
ldb_open_native(value s, value write_buffer_size, value max_open_files,
                value block_size, value block_restart_interval,
                value cache_size_option, value comparator, value env)
{
 CAMLparam1(s);
 CAMLlocal1(r);

 leveldb::Options options;

 leveldb::DB* db;
 options.create_if_missing = true;
 options.write_buffer_size = Long_val(write_buffer_size);
 options.max_open_files = Int_val(max_open_files);
 options.block_size = Int_val(block_size);
 options.block_restart_interval = Int_val(block_restart_interval);

 if (env != Val_none) options.env = (leveldb::Env *)env;

 if (cache_size_option != Val_none) {
   options.block_cache = leveldb::NewLRUCache(Int_val(Field(cache_size_option, 0)) * 1048576);
 }
 options.comparator = (const leveldb::Comparator *)comparator;
 leveldb::Status status = leveldb::DB::Open(options, String_val(s), &db);
 CHECK_ERROR(status);

 WRAP(r, db, DB, false);
 CAMLreturn(r);
}

CAMLprim value
ldb_open_bytecode(value *argv, int argn)
{
  return ldb_open_native(argv[0], argv[1], argv[2], argv[3], argv[4],
                         argv[5], argv[6], argv[7]);
}

CAMLprim value
ldb_close(value t)
{
  LDB_ANY(t)->auto_finalize = true;
  ldb_any_finalize(t);
  return(Val_unit);
}

CAMLprim value
ldb_destroy(value s)
{
 std::string _s(String_val(s), string_length(s));

 caml_enter_blocking_section();
 leveldb::Status status = leveldb::DestroyDB(_s, leveldb::Options());
 caml_leave_blocking_section();
 return(status.ok() ? Val_true : Val_false);
}

CAMLprim value
ldb_repair(value s)
{
 std::string _s(String_val(s), string_length(s));

 caml_enter_blocking_section();
 leveldb::Status status = leveldb::RepairDB(_s, leveldb::Options());
 caml_leave_blocking_section();
 return(status.ok() ? Val_true : Val_false);
}

#define TO_SLICE(x) leveldb::Slice(String_val(x), string_length(x))

#define TO_SLICE_COPY(dst, x) \
    char _data_##x##_[string_length(x)]; \
    memcpy(_data_##x##_, String_val(x), string_length(x)); \
    leveldb::Slice dst(_data_##x##_, string_length(x));

#define COPY_FROM(dst, src) \
    do { \
        dst = caml_alloc_string(src.size()); \
        memcpy(Bytes_val(dst), src.data(), src.size()); \
    } while(0);

CAMLprim value
ldb_get(value t, value k)
{
 using namespace std;
 CAMLparam2(t, k);
 CAMLlocal1(ret);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);

 TO_SLICE_COPY(key, k);

 caml_enter_blocking_section();
 std::string v;
 leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &v);
 // must release before recovering the OCaml runtime lock because otherwise
 // we could be stuck waiting for it while another thread running the GC is
 // waiting for us to release the handle
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 if(status.IsNotFound()) { v.~string(); status.~Status(); RAISE_NOT_FOUND; }

 CHECK_ERROR_AND_CLEANUP(status, { v.~string(); });

 COPY_FROM(ret, v);
 CAMLreturn(ret);
}

CAMLprim value
ldb_put(value t, value k, value v, value sync)
{
 using namespace std;
 CAMLparam3(t, k, v);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);
 TO_SLICE_COPY(key, k);

 std::string val(String_val(v), string_length(v));

 leveldb::WriteOptions options;

 options.sync = (Val_true == sync);

 caml_enter_blocking_section();
 leveldb::Status status = db->Put(options, key, val);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 CHECK_ERROR_AND_CLEANUP(status, { val.~string(); });

 CAMLreturn(Val_unit);
}


CAMLprim value
ldb_delete(value t, value k, value sync)
{
 CAMLparam2(t, k);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);

 TO_SLICE_COPY(key, k);

 leveldb::WriteOptions options;

 options.sync = (Val_true == sync);

 caml_enter_blocking_section();
 leveldb::Status status = db->Delete(options, key);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 CHECK_ERROR(status);

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_mem(value t, value k)
{
 using namespace std;
 CAMLparam2(t, k);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);

 TO_SLICE_COPY(key, k);
 std::string v;

 caml_enter_blocking_section();
 leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &v);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 if(status.IsNotFound()) CAMLreturn(Val_false);
 if(status.ok ()) CAMLreturn(Val_true);

 CHECK_ERROR_AND_CLEANUP(status, { v.~string(); });

 CAMLreturn(Val_false);
}

CAMLprim value
ldb_iterator_compare(value t1, value t2)
{
 return Val_int(ldb_any_compare(t1, t2));
}

CAMLprim value
ldb_make_iter(value t, value fill_cache)
{
 CAMLparam1(t);
 CAMLlocal1(it);

 leveldb::DB *db = LDB_HANDLE(t);

 CHECK_CLOSED(t);
 leveldb::ReadOptions options;
 options.fill_cache = Bool_val(fill_cache);
 leveldb::Iterator *_it = db->NewIterator(options);

 WRAP(it, _it, Iterator, false);
 CAMLreturn(it);
}

CAMLprim value
ldb_iter_close(value t)
{
 LDB_ANY(t)->auto_finalize = true;
 ldb_any_finalize(t);
 return(Val_unit);
}

CAMLprim value
ldb_it_first(value it)
{
 CAMLparam1(it);
 USE_IT(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 caml_enter_blocking_section();
 _it->SeekToFirst();
 RELEASE_IT(it);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_it_last(value it)
{
 CAMLparam1(it);
 USE_IT(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 caml_enter_blocking_section();
 _it->SeekToLast();
 RELEASE_IT(it);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_it_seek_unsafe(value it, value s, value off, value len)
{
 CAMLparam2(it, s);
 USE_IT(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 char key_data[Int_val(len)];
 memcpy(key_data, &Byte_u(s, Int_val(off)), Int_val(len));
 leveldb::Slice key(key_data, Int_val(len));

 caml_enter_blocking_section();
 _it->Seek(key);
 RELEASE_IT(it);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_it_next(value it)
{
 CAMLparam1(it);
 USE_IT(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 caml_enter_blocking_section();
 _it->Next();
 RELEASE_IT(it);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_it_prev(value it)
{
 CAMLparam1(it);
 USE_IT(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 caml_enter_blocking_section();
 _it->Prev();
 RELEASE_IT(it);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_it_valid(value it)
{
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 if(LDB_ANY(it)->closed || !_it || !_it->Valid()) return Val_false;

 return Val_true;
}

/* returns:
 * SIZE if the key exists and its size is SIZE
 * if SIZE <= buf len, the key is copied into the supplied buffer
 */
CAMLprim value
ldb_it_key_unsafe(value it, value buf)
{
 CAMLparam2(it, buf);

 CHECK_IT_CLOSED(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 if(!_it->Valid()) raise_status_error_and_release(_it->status());

 leveldb::Slice key = _it->key();
 size_t size = key.size();

 if(size <= string_length(buf))
     memcpy(Bytes_val(buf), key.data(), size);

 CAMLreturn(Val_long(size));
}

/* returns:
 * SIZE if the value exists and its size is SIZE
 * if SIZE <= buf len, the value is copied into the supplied buffer
 */
CAMLprim value
ldb_it_value_unsafe(value it, value buf)
{
 CAMLparam2(it, buf);

 CHECK_IT_CLOSED(it);
 leveldb::Iterator *_it = LDB_ITERATOR(it);

 if(!_it->Valid()) raise_status_error_and_release(_it->status());

 leveldb::Slice v = _it->value();
 size_t size = v.size();

 if(size <= string_length(buf))
     memcpy(Bytes_val(buf), v.data(), size);

 CAMLreturn(Val_long(size));
}

CAMLprim value
ldb_writebatch_make(value unit)
{
 CAMLparam0();
 CAMLlocal1(ret);

 leveldb::WriteBatch *b = new leveldb::WriteBatch;
 WRAP(ret, b, WriteBatch, true);

 CAMLreturn(ret);
}

CAMLprim value
ldb_writebatch_put_substring_unsafe_native
(value t, value k, value o1, value l1, value v, value o2, value l2)
{
 leveldb::WriteBatch *b = LDB_WRITEBATCH(t);

 leveldb::Slice key = leveldb::Slice(String_val(k) + Int_val(o1), Int_val(l1));
 leveldb::Slice value = leveldb::Slice(String_val(v) + Int_val(o2), Int_val(l2));
 b->Put(key, value);

 return Val_unit;
}

CAMLprim value
ldb_writebatch_put_substring_unsafe_bytecode(value *argv, int argn)
{
 return
     ldb_writebatch_put_substring_unsafe_native(argv[0], argv[1], argv[2],
                                                argv[3], argv[4], argv[5],
                                                argv[6]);
}

CAMLprim value
ldb_writebatch_delete_substring_unsafe(value t, value k, value off, value len)
{
 leveldb::WriteBatch *b = LDB_WRITEBATCH(t);

 leveldb::Slice key = leveldb::Slice(String_val(k) + Int_val(off), Int_val(len));
 b->Delete(key);

 return Val_unit;
}

CAMLprim value
ldb_write_batch(value t, value batch, value sync)
{
 CAMLparam2(t, batch);
 leveldb::DB *db = LDB_HANDLE(t);
 leveldb::WriteBatch *b = LDB_WRITEBATCH(batch);

 USE_HANDLE(t);
 leveldb::WriteOptions options;
 options.sync = (Val_true == sync);

 caml_enter_blocking_section();
 leveldb::Status status = db->Write(options, b);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 CHECK_ERROR(status);

 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_get_approximate_size(value t, value _from, value _to)
{
 CAMLparam3(t, _from, _to);
 CAMLlocal1(ret);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);

 TO_SLICE_COPY(__from, _from);
 TO_SLICE_COPY(__to, _to);

 leveldb::Range range(__from, __to);
 uint64_t size;

 caml_enter_blocking_section();
 db->GetApproximateSizes(&range, 1, &size);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 ret = caml_copy_int64(size);
 CAMLreturn(ret);
}

CAMLprim value
ldb_get_property(value t, value s)
{
 CAMLparam2(t, s);
 CAMLlocal2(ret, retstring);
 std::string v;

 CHECK_CLOSED(t);
 bool found = LDB_HANDLE(t)->GetProperty(TO_SLICE(s), &v);

 if(!found) CAMLreturn(Val_unit);

 COPY_FROM(retstring, v);
 ret = caml_alloc_small(1, 0);
 Field(ret, 0) = retstring;

 CAMLreturn(ret);
}

CAMLprim value
ldb_compact_range(value t, value begin, value end)
{
 CAMLparam3(t, begin, end);
 leveldb::DB *db = LDB_HANDLE(t);

 USE_HANDLE(t);

#define CPP_STRING(x) std::string(String_val(x), string_length(x))

 std::string begin_s =
     Is_block(begin) ? CPP_STRING(Field(begin, 0)) : std::string("");

 std::string end_s =
     Is_block(end) ? CPP_STRING(Field(end, 0)) : std::string("");

 leveldb::Slice begin_(begin_s), end_(end_s);

 caml_enter_blocking_section();
 db->CompactRange(Is_block(begin) ? &begin_ : NULL,
                  Is_block(end) ? &end_ : NULL);
 RELEASE_HANDLE(t);
 caml_leave_blocking_section();

 CAMLreturn(Val_unit);
}


static void
ldb_snapshot_finalize(value t)
{
 ldb_snapshot *s = UNWRAP_SNAPSHOT(t);

 s->closed = true;

 // wait until the resource is not being used in another thread
 while(s->in_use && *(s->in_use)) {
     struct timeval tv;
     tv.tv_sec = 0;
     tv.tv_usec = 1000;
     select(0, NULL, NULL, NULL, &tv);
 }
 if(s->snapshot) {
     s->db->ReleaseSnapshot(s->snapshot);
     s->snapshot = NULL;
 }
 if(s->in_use) {
     free(s->in_use);
     s->in_use = NULL;
 }
}

CAMLprim value
ldb_snapshot_compare(value t1, value t2)
{
 return Val_int(ldb_any_compare(t1, t2));
}

CAMLprim value
ldb_snapshot_make(value t)
{
 CAMLparam1(t);
 CAMLlocal1(ret);

 CHECK_CLOSED(t);
 leveldb::DB *db = LDB_HANDLE(t);
 const leveldb::Snapshot* snapshot = db->GetSnapshot();
 ret = caml_alloc_custom(&ldb_snapshot_ops, sizeof(ldb_snapshot), 0, 1);
 ldb_snapshot *_ret = UNWRAP_SNAPSHOT(ret);
 _ret->id = ++wrapped_val_id;
 _ret->db = db;
 _ret->snapshot = snapshot;
 _ret->closed = false;
 _ret->in_use = (bool *)malloc(sizeof(bool));
 *(_ret->in_use) = false;
 CAMLreturn(ret);
}

CAMLprim value
ldb_snapshot_release(value t)
{
 CAMLparam1(t);
 ldb_snapshot_finalize(t);
 CAMLreturn(Val_unit);
}

CAMLprim value
ldb_snapshot_get(value t, value k)
{
 using namespace std;
 CAMLparam2(t, k);
 CAMLlocal1(ret);

 USE_SNAPSHOT(t);
 ldb_snapshot *snap = UNWRAP_SNAPSHOT(t);
 leveldb::DB *db = snap->db;

 TO_SLICE_COPY(key, k);
 leveldb::ReadOptions options;

 options.snapshot = snap->snapshot;

 caml_enter_blocking_section();
 std::string v;
 leveldb::Status status = db->Get(options, key, &v);
 RELEASE_SNAPSHOT(t);
 caml_leave_blocking_section();

 if(status.IsNotFound()) { v.~string(); status.~Status(); RAISE_NOT_FOUND; }

 CHECK_ERROR_AND_CLEANUP(status, { v.~string(); });

 COPY_FROM(ret, v);
 CAMLreturn(ret);
}

CAMLprim value
ldb_snapshot_mem(value t, value k)
{
 using namespace std;
 CAMLparam2(t, k);

 USE_SNAPSHOT(t);
 ldb_snapshot *snap = UNWRAP_SNAPSHOT(t);
 leveldb::DB *db = snap->db;

 TO_SLICE_COPY(key, k);
 leveldb::ReadOptions options;

 options.snapshot = snap->snapshot;

 std::string v;

 caml_enter_blocking_section();
 leveldb::Status status = db->Get(options, key, &v);
 RELEASE_SNAPSHOT(t);
 caml_leave_blocking_section();

 if(status.IsNotFound()) CAMLreturn(Val_false);
 if(status.ok ()) CAMLreturn(Val_true);

 CHECK_ERROR_AND_CLEANUP(status, { v.~string(); });

 CAMLreturn(Val_false);
}

CAMLprim value
ldb_snapshot_make_iterator(value t)
{
 CAMLparam1(t);
 CAMLlocal1(it);

 CHECK_SNAPSHOT_CLOSED(t);
 leveldb::DB *db = UNWRAP_SNAPSHOT(t)->db;

 leveldb::ReadOptions options;
 options.snapshot = UNWRAP_SNAPSHOT(t)->snapshot;
 leveldb::Iterator *_it = db->NewIterator(options);

 WRAP(it, _it, Iterator, false);
 CAMLreturn(it);
}

}
