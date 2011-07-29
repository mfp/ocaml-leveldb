(** Access to [leveldb] databases. *)

(** {2 Exceptions} *)

(** Errors (apart from [Not_found]) are notified with [Error s] exceptions. *)
exception Error of string

(** {2 Types} *)

(** Database *)
type db

(** Database iterators. *)
type iterator

(** Batch write operations. *)
type writebatch

(** Immutable database snapshots. *)
type snapshot

(** Read-only access to the DB or a snapshot. *)
type read_access

(** Type that represents a [const Comparator*] pointer (refer to
  * LevelDB's [comparator.h]). If you want to define your own,
  * use an external function of type [unit -> comparator]
  * returning the pointer.*)
type comparator

(** {2 Database maintenance} *)

(** Destroy the contents of the database in the given directory.
  * @return [true] if the operation succeeded. *)
val destroy : string -> bool

(** If a DB cannot be opened, you may attempt to call this method to resurrect
  * as much of the contents of the database as possible.  Some data may be
  * lost, so be careful when calling this function on a database that contains
  * important information.
  * @return [true] if the operation succeeded. *)
val repair : string -> bool

(** {2 Database operations} *)

val lexicographic_comparator : comparator

(** Open a leveldb database in the given directory. *)
val open_db :
  ?write_buffer_size:int ->
  ?max_open_files:int ->
  ?block_size:int -> ?block_restart_interval:int ->
  ?comparator:comparator ->
  string -> db

(** Close the database. All further operations on it will fail.
  * Existing snapshots and iterators are released and invalidated.
  * Note that the database is closed automatically in the finalizer if you
  * don't close it manually. *)
val close : db -> unit

(** Read-only access to the DB. *)
val read_access : db -> read_access

(** Return a new iterator. *)
val iterator : db -> iterator

(** [get_approximate_size from_key to_key] returns the approximate size
  * on disk of the range comprised between [from_key] and [to_key]. *)
val get_approximate_size : db -> string -> string -> Int64.t

(** Return the specified property, if existent. *)
val get_property : db -> string -> string option

(** {2 Read/write} *)

(** Retrieve a value. *)
val get : db -> string -> string option

(** Retrieve a value, raising [Not_found] if missing. *)
val get_exn : db -> string -> string

(** [mem db key] returns [true] iff [key] is present in [db]. *)
val mem : db -> string -> bool

(** [put ?sync key value] adds (or replaces) a binding to the database.
  * @param sync whether to write synchronously (default: false) *)
val put : db -> ?sync:bool -> string -> string -> unit

(** [put_and_snapshot ?sync key value] proceeds like [put ?sync key value] and
  * returns a snapshot of the database immediately after the write. *)
val put_and_snapshot : db -> ?sync:bool -> string -> string -> snapshot

(** [delete ?sync key] deletes the binding for the given key.
  * @param sync whether to write synchronously (default: false) *)
val delete : db -> ?sync:bool -> string -> unit

(** Proceed like {!delete}, and return a snapshot of the database immediately
  * after the write. *)
val delete_and_snapshot : db -> ?sync:bool -> string -> snapshot

(** {3 Iteration} *)

(** [iter f db] applies [f] to all the bindings in [db] until it returns
  * [false], i.e.  runs [f key value] for all the bindings in lexicographic
  * key order. *)
val iter : (string -> string -> bool) -> db -> unit

(** Like {!iter}, but proceed in reverse lexicographic order. *)
val rev_iter : (string -> string -> bool) -> db -> unit

(** [iter_from f db start] applies [f key value] for all the bindings after
  * [start] (inclusive) until it returns false. *)
val iter_from : (string -> string -> bool) -> db -> string -> unit

(** [iter_from f db start] applies [f key value] for all the bindings before
  * [start] (inclusive) in reverse lexicographic order until [f] returns
  * [false].. *)
val rev_iter_from : (string -> string -> bool) -> db -> string -> unit

(** {2 Batch operations} *)

(** Batch operations applied atomically. *)
module Batch :
sig
  (** Initialize a batch operation. *)
  val make : unit -> writebatch

  (** [put writebatch key value] adds or replaces a binding. *)
  val put : writebatch -> string -> string -> unit

  (** [put_substring writebatch key off1 len1 value off2 len2] adds or
    * replaces a binding for the substrings of [key] and [value] delimited by
    * the given offsets and lengths.
    * @raise Error if the offset, length pairs do not represent valid
    * substrings *)
  val put_substring : writebatch ->
    string -> int -> int ->
    string -> int -> int -> unit

  (** [delete writebatch key] removes the binding for [key], if present.. *)
  val delete : writebatch -> string -> unit

  (** [delete writebatch s off len] removes (if present) the binding for the
    * substring of [s] delimited by the offset [off] and the length [len]. *)
  val delete_substring : writebatch -> string -> int -> int -> unit

  (** Apply the batch operation atomically.
    * @param sync whether to write synchronously (default: false) *)
  val write : db -> ?sync:bool -> writebatch -> unit

  (** Proceed like {!write}, and return a snapshot of the database immediately
    * after the write. *)
  val write_and_snapshot : db -> ?sync:bool -> writebatch -> snapshot
end

(** {2 Iterators} *)

(** Iteration over bindings in a database. *)
module Iterator :
sig
  (** Create a new iterator. Note that the iterator keeps a reference to the
    * DB, so the latter will not be GCed automatically as long as the iterator
    * is being used. Note also that if the DB is closed manually, the iterator
    * will be invalidated and further operations will fail.  The returned
    * iterator needs not be closed manually, for it will be closed in its
    * finalizer. *)
  val make : db -> iterator

  (** Close the iterator. Further operations on it will fail. Note that
    * the iterator fill be closed automatically in its finalizer if this
    * function is not called manually. *)
  val close : iterator -> unit

  (** Jump the the first binding in the database/snapshot. *)
  val seek_to_first : iterator -> unit

  (** Jump the the last binding in the database/snapshot. *)
  val seek_to_last : iterator -> unit

  (** [seek it s off len] seeks to first binding whose key is >= to the key
    * corresponding to the substring of [s] starting at [off] and of length
    * [len].
    * @raise Error if the offset/length does not represent a substring of the
    * key. *)
  val seek: iterator -> string -> int -> int -> unit

  (** Jump to the next binding. *)
  val next : iterator -> unit

  (** Jump to the previous binding. *)
  val prev : iterator -> unit

  (** @return true iff the iterator is pointing to a binding. *)
  val valid : iterator -> bool

  (** [fill_key it r] places the key for the current binding in the string
    * referred to by [r] if it fits, otherwise it creates a new string and
    * updates the reference.
    * @raise Error if the iterator is not {!valid}
    * @return length of the key *)
  val fill_key : iterator -> string ref -> int

  (** Similar to {!fill_key}, but returning the value. *)
  val fill_value : iterator -> string ref -> int

  (** Return the key part of the binding pointer to by the iterator.
    * @raise Error if the iterator is not {!valid}. *)
  val get_key : iterator -> string

  (** Return the value part of the binding pointer to by the iterator.
    * @raise Error if the iterator is not {!valid}. *)
  val get_value : iterator -> string

  (** [iter f db] applies [f] to all the bindings in the database/snapshot the
    * iterator belongs to, until [f] returns [false], i.e.  runs [f key value]
    * for all the bindings in lexicographic key order. *)
  val iter : (string -> string -> bool) -> iterator -> unit

  (** Like {!iter}, but proceed in reverse lexicographic order. *)
  val rev_iter : (string -> string -> bool) -> iterator -> unit

  (** [iter_from f it start] applies [f key value] for all the bindings after
    * [start] (inclusive) until it returns false. *)
  val iter_from : (string -> string -> bool) -> iterator -> string -> unit

  (** [iter_from f it start] applies [f key value] for all the bindings before
    * [start] (inclusive) in reverse lexicographic order until [f] returns
    * [false].. *)
  val rev_iter_from : (string -> string -> bool) -> iterator -> string -> unit
end

(** {2 Snapshots} *)

(** Access to database snapshots. *)
module Snapshot :
sig
  (** Create a new snapshot. Note that the snapshot keeps a reference to the
    * DB, so the latter will not be GCed automatically as long as the snapshot
    * is being used. Note also that if the DB is closed manually, the snapshot
    * will be released and further operations will fail.  The returned
    * snapshot needs not be released manually, for it will be released in its
    * finalizer. *)
  val make : db -> snapshot

  (** Release the finalizer. Further operations on it will fail. Note that
    * the snapshot fill be released automatically in its finalizer if this
    * function is not called manually. *)
  val release : snapshot -> unit

  val get : snapshot -> string -> string option
  val get_exn : snapshot -> string -> string

  val mem : snapshot -> string -> bool

  (** Return a new iterator. *)
  val iterator : snapshot -> iterator

  val read_access : snapshot -> read_access

  (** Refer to {!Iterator.iter}. *)
  val iter : (string -> string -> bool) -> snapshot -> unit

  (** Refer to {!Iterator.rev_iter}. *)
  val rev_iter : (string -> string -> bool) -> snapshot -> unit

  (** Refer to {!Iterator.iter_from}. *)
  val iter_from : (string -> string -> bool) -> snapshot -> string -> unit

  (** Refer to {!Iterator.rev_iter_from}. *)
  val rev_iter_from : (string -> string -> bool) -> snapshot -> string -> unit
end

(** {2 Abstract read-only access} *)

(** Read-only access to databases and snapshots. *)
module Read_access :
sig
  val get : read_access -> string -> string option
  val get_exn : read_access -> string -> string
  val mem : read_access -> string -> bool
  val iterator : read_access -> iterator

  (** Refer to {!Iterator.iter}. *)
  val iter : (string -> string -> bool) -> read_access -> unit

  (** Refer to {!Iterator.rev_iter}. *)
  val rev_iter : (string -> string -> bool) -> read_access -> unit

  (** Refer to {!Iterator.iter_from}. *)
  val iter_from : (string -> string -> bool) -> read_access -> string -> unit

  (** Refer to {!Iterator.rev_iter_from}. *)
  val rev_iter_from : (string -> string -> bool) -> read_access -> string -> unit
end
