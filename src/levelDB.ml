(*
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
 *)

type db_
type snapshot_
type writebatch
type iterator_
type comparator

external compare_snapshot_ :
  snapshot_ -> snapshot_ -> int = "ldb_snapshot_compare" "noalloc"

external compare_iterator_ :
  iterator_ -> iterator_ -> int = "ldb_iterator_compare" "noalloc"

module rec TYPES :
sig
  module SNAPSHOTS : Set.S with type elt = TYPES.snapshot
  module ITERATORS : Set.S with type elt = TYPES.iterator

  type db =
      { db : db_;
        mutable snapshots : SNAPSHOTS.t;
        mutable iterators : ITERATORS.t
      }
  type snapshot = { s_parent : db; s_handle : snapshot_ }
  type iterator = { i_parent : db; i_handle : iterator_ }
end =
struct
  module SNAPSHOTS =
    Set.Make(struct
               type t = TYPES.snapshot
               let compare s1 s2 =
                 compare_snapshot_ s1.TYPES.s_handle s2.TYPES.s_handle
             end)

  module ITERATORS =
    Set.Make(struct
               type t = TYPES.iterator
               let compare i1 i2 =
                 compare_iterator_ i1.TYPES.i_handle i2.TYPES.i_handle
             end)

  type db =
      { db : db_;
        mutable snapshots : SNAPSHOTS.t;
        mutable iterators : ITERATORS.t
      }

  type snapshot = { s_parent : db; s_handle : snapshot_ }
  type iterator = { i_parent : db; i_handle : iterator_ }
end

include TYPES

type read_access =
    {
      get_exn : string -> string;
      mem : string -> bool;
      iterator : unit -> iterator;
    }

exception Error of string

let () =
  Callback.register_exception "org.eigenclass/leveldb/Not_found" Not_found;
  Callback.register_exception "org.eigenclass/leveldb/Error" (Error "")

let error fmt =
  Printf.ksprintf (fun s -> raise (Error s)) fmt

external destroy : string -> bool = "ldb_destroy"
external repair : string -> bool = "ldb_repair"

external lexicographic_comparator : unit -> comparator =
  "ldb_lexicographic_comparator" "noalloc"

let lexicographic_comparator = lexicographic_comparator ()

external open_db_ :
  string -> write_buffer_size:int -> max_open_files:int ->
  block_size:int  -> block_restart_interval:int ->
  comparator:comparator -> db_ = "ldb_open_bytecode" "ldb_open_native"

external close_ : db_ -> unit = "ldb_close"
external get_exn_ : db_ -> string -> string = "ldb_get"

external put_ : db_ -> string -> string -> sync:bool -> snapshot:bool ->
  snapshot_ option = "ldb_put"

external delete_ : db_ -> string -> sync:bool -> snapshot:bool ->
  snapshot_ option = "ldb_delete"

external mem_ : db_ -> string -> bool = "ldb_mem"

external get_approximate_size_ : db_ -> string -> string -> Int64.t =
  "ldb_get_approximate_size"

external get_property_ : db_ -> string -> string option =
  "ldb_get_property"

external release_snapshot_ : snapshot_ -> unit = "ldb_snapshot_release"
external close_iterator_ : iterator_ -> unit = "ldb_iter_close"

let release_snapshot s =
  s.s_parent.snapshots <- SNAPSHOTS.remove s s.s_parent.snapshots;
  release_snapshot_ s.s_handle

let close_iterator it =
  it.i_parent.iterators <- ITERATORS.remove it it.i_parent.iterators;
  close_iterator_ it.i_handle

let add_snapshot_to_db s_parent s_handle =
  let s = { s_handle; s_parent } in
    s_parent.snapshots <- SNAPSHOTS.add s s_parent.snapshots;
    Gc.finalise release_snapshot s;
    s

let add_iterator_to_db db handle =
  let it = { i_handle = handle; i_parent = db } in
    db.iterators <- ITERATORS.add it db.iterators;
    Gc.finalise close_iterator it;
    it

module Batch =
struct
  external make : unit -> writebatch = "ldb_writebatch_make"

  external put_substring_unsafe :
    writebatch ->
    string -> int -> int ->
    string -> int -> int ->
    unit =
      "ldb_writebatch_put_substring_unsafe_bytecode"
      "ldb_writebatch_put_substring_unsafe_native" "noalloc"

  let put b k v =
    put_substring_unsafe
      b k 0 (String.length k) v 0 (String.length v)

  let put_substring b k o1 l1 v o2 l2 =
    if o1 < 0 || l1 < 0 || o1 + l1 > String.length k then
      error "Snapshot.put_substring: invalid key substring";
    if o2 < 0 || l2 < 0 || o2 + l2 > String.length v then
      error "Snapshot.put_substring: invalid value substring";
    put_substring_unsafe b k o1 l1 v o2 l2

  external delete_substring_unsafe :
    writebatch -> string -> int -> int -> unit =
      "ldb_writebatch_delete_substring_unsafe" "noalloc"

  let delete b k = delete_substring_unsafe b k 0 (String.length k)

  let delete_substring b k off len =
    if off < 0 || len < 0 || off + len > String.length k then
      error "Snapshot.delete_substring: invalid key substring";
    delete_substring_unsafe b k off len

  external write : db_ -> writebatch -> sync:bool -> snapshot:bool ->
    snapshot_ option = "ldb_write_batch"

  let write_and_snapshot db ?(sync = false) writebatch =
    match write db.db writebatch ~sync ~snapshot:true with
        None -> assert false
      | Some s_handle -> add_snapshot_to_db db s_handle

  let write db ?(sync = false) writebatch =
    ignore (write db.db writebatch ~sync ~snapshot:false)
end

module Iterator =
struct
  external make_ : db_ -> iterator_ = "ldb_make_iter"
  external seek_to_first_ : iterator_ -> unit = "ldb_it_first"
  external seek_to_last_ : iterator_ -> unit = "ldb_it_last"

  external seek_unsafe_ : iterator_ -> string -> int -> int -> unit =
    "ldb_it_seek_unsafe"

  external next_ : iterator_ -> unit = "ldb_it_next"
  external prev_: iterator_ -> unit = "ldb_it_prev"

  external valid_ : iterator_ -> bool = "ldb_it_valid" "noalloc"

  external key_unsafe_ : iterator_ -> string -> int = "ldb_it_key_unsafe"
  external value_unsafe_ : iterator_ -> string -> int = "ldb_it_value_unsafe"

  let close = close_iterator

  let make db = add_iterator_to_db db (make_ db.db)

  let seek_to_first it = seek_to_first_ it.i_handle
  let seek_to_last it = seek_to_last_ it.i_handle

  let next it = next_ it.i_handle
  let prev it = prev_ it.i_handle
  let valid it = valid_ it.i_handle

  let seek it s off len =
    if off < 0 || len < 0 || off + len > String.length s then
      error "Iterator.seek: invalid arguments (key:%S off:%d len:%d)"
        s off len;
    seek_unsafe_ it.i_handle s off len

  let fill_and_resize_if_needed name f it buf =
    let len = f it !buf in
      if len <= String.length !buf then len
      else begin
        if len > Sys.max_string_length then
          error "Iterator.%s: string is larger than Sys.max_string_length" name;
        buf := String.create len;
        f it !buf
      end

  let fill_key it buf =
    fill_and_resize_if_needed "fill_key" key_unsafe_ it.i_handle buf

  let fill_value it buf =
    fill_and_resize_if_needed "fill_value" value_unsafe_ it.i_handle buf

  let get_key it = let b = ref "" in ignore (fill_key it b); !b
  let get_value it = let b = ref "" in ignore (fill_value it b); !b

  let iter_aux next f it =
    let finished = ref false in
      while not !finished && valid it do
        finished := not (f (get_key it) (get_value it));
        next it
      done

  let iter f it =
    seek_to_first it;
    iter_aux next f it

  let rev_iter f it =
    seek_to_last it;
    iter_aux prev f it

  let iter_from f it k =
    seek it k 0 (String.length k);
    iter_aux next f it

  let rev_iter_from f it k =
    seek it k 0 (String.length k);
    iter_aux prev f it
end

module Read_access =
struct
  let get t k = try Some (t.get_exn k) with Not_found -> None
  let get_exn t k = t.get_exn k
  let mem t k = t.mem k
  let iterator t = t.iterator ()

  let iter f t = Iterator.iter f (iterator t)
  let rev_iter f t = Iterator.rev_iter f (iterator t)
  let iter_from f t k = Iterator.iter_from f (iterator t) k
  let rev_iter_from f t k = Iterator.rev_iter_from f (iterator t) k
end

module Snapshot =
struct
  external make_ : db_ -> snapshot_ = "ldb_snapshot_make"
  external get_exn_ : snapshot_ -> string -> string = "ldb_snapshot_get"
  external mem_ : snapshot_ -> string -> bool = "ldb_snapshot_mem"
  external iterator_ : snapshot_ -> iterator_ = "ldb_snapshot_make_iterator"

  let make db =
    let s_handle = make_ db.db in
    let s = add_snapshot_to_db db s_handle in
      s

  let release = release_snapshot

  let get_exn s k = get_exn_ s.s_handle k
  let mem s k = mem_ s.s_handle k

  let iterator s =
    let it = { i_handle = iterator_ s.s_handle; i_parent = s.s_parent } in
      s.s_parent.iterators <- ITERATORS.add it s.s_parent.iterators;
      it

  let get t k = try Some (get_exn t k) with Not_found -> None

  let read_access s =
    { get_exn = get_exn s; mem = mem s;
      iterator = (fun () -> iterator s) }

  let iter f t = Iterator.iter f (iterator t)
  let rev_iter f t = Iterator.rev_iter f (iterator t)
  let iter_from f t k = Iterator.iter_from f (iterator t) k
  let rev_iter_from f t k = Iterator.rev_iter_from f (iterator t) k
end

let close db =
  SNAPSHOTS.iter Snapshot.release db.snapshots;
  ITERATORS.iter Iterator.close db.iterators;
  close_ db.db

let open_db
      ?(write_buffer_size = 4*1024*1024)
      ?(max_open_files = 1000)
      ?(block_size = 4096)
      ?(block_restart_interval = 16)
      ?(comparator = lexicographic_comparator)
      path =
  let db = open_db_
    ~write_buffer_size ~max_open_files ~block_size ~block_restart_interval
    ~comparator path in
  let db = { db; snapshots = SNAPSHOTS.empty; iterators = ITERATORS.empty; } in
    Gc.finalise close db;
    db

let get_exn db k = get_exn_ db.db k

let get db k = try Some (get_exn db k) with Not_found -> None

let mem db k = mem_ db.db k

let read_access db =
  { get_exn = get_exn db; mem = mem db;
    iterator = (fun () -> Iterator.make db) }

let iterator db = Iterator.make db

let delete_and_snapshot db ?(sync = false) k =
  match delete_ db.db ~sync ~snapshot:true k with
      None -> assert false
    | Some s -> add_snapshot_to_db db s

let delete db ?(sync = false) k =
  ignore (delete_ db.db ~sync ~snapshot:false k)

let put_and_snapshot db ?(sync = false) k v =
  match put_ db.db ~sync ~snapshot:true k v with
      None -> assert false
    | Some s -> add_snapshot_to_db db s

let put db ?(sync = false) k v =
  ignore (put_ db.db ~sync ~snapshot:false k v)

let iter f db = Iterator.iter f (Iterator.make db)
let rev_iter f db = Iterator.rev_iter f (Iterator.make db)
let iter_from f db k = Iterator.iter_from f (Iterator.make db) k
let rev_iter_from f db k = Iterator.rev_iter_from f (Iterator.make db) k

let get_approximate_size db k1 k2 = get_approximate_size_ db.db k1 k2
let get_property db k = get_property_ db.db k
