(** Access to [leveldb] databases. *)

(** Errors (apart from [Not_found]) are notified with [Error s] exceptions. *)
exception Error of string

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

(** Destroy the contents of the database in the given directory.
  * @return [true] if the operation succeeded. *)
val destroy : string -> bool

(** If a DB cannot be opened, you may attempt to call this method to resurrect
  * as much of the contents of the database as possible.  Some data may be
  * lost, so be careful when calling this function on a database that contains
  * important information.
  * @return [true] if the operation succeeded. *)
val repair : string -> bool

(** Open a leveldb database in the given directory. *)
val open_db :
  ?write_buffer_size:int ->
  ?max_open_files:int ->
  ?block_size:int -> ?block_restart_interval:int -> string -> db

(** Close the database. All further operations on it will fail.
  * Note that the database is closed automatically in the finalizer if you
  * don't close it manually. *)
val close : db -> unit

(** [get_approximate_size from_key to_key] returns the approximate size
  * on disk of the range comprised between [from_key] and [to_key]. *)
val get_approximate_size : db -> string -> string -> Int64.t

(** Return the specified property, if existent. *)
val get_property : db -> string -> string option

(** Read-only access to the DB. *)
val read_access : db -> read_access

(** Retrieve a value. *)
val get : db -> string -> string option

(** Retrieve a value, raising [Not_found] if missing. *)
val get_exn : db -> string -> string

(** [put ?sync key value] adds (or replaces) a binding to the database.
  * @param sync whether to write synchronously (default: false) *)
val put : db -> ?sync:bool -> string -> string -> unit

(** [put_and_snapshot ?sync key value] proceeds like [put ?sync key value] and
  * returns a snapshot of the database immediately after the write. *)
val put_and_snapshot : db -> ?sync:bool -> string -> string -> snapshot

(** [delete ?sync key] deletes the binding for the given key.
  * @param sync whether to write synchronously (default: false) *)
val delete : db -> ?sync:bool -> string -> unit

val delete_and_snapshot : db -> ?sync:bool -> string -> snapshot

(** [mem db key] returns [true] iff [key] is present in [db]. *)
val mem : db -> string -> bool

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

(** Batch operations applied atomically. *)
module Batch :
sig
  (** Initialize a batch operation. *)
  val make : unit -> writebatch

  (** [put writebatch key value] adds or replaces a binding. *)
  val put : writebatch -> string -> string -> unit

  (** [delete writebatch key] removes the binding for [key], if present.. *)
  val delete : writebatch -> string -> unit

  (** Apply the batch operation atomically.
    * @param sync whether to write synchronously (default: false) *)
  val write : db -> ?sync:bool -> writebatch -> unit

  val write_and_snapshot : db -> ?sync:bool -> writebatch -> snapshot
end

(** Iteration over bindings in a database. *)
module Iterator :
sig
  val make : db -> iterator
  val close : iterator -> unit

  val seek_to_first : iterator -> unit
  val seek_to_last : iterator -> unit

  (** [seek it s off len] seeks to first binding whose key is >= to the key
    * corresponding to the substring of [s] starting at [off] and of length
    * [len] *)
  val seek: iterator -> string -> int -> int -> unit

  val next : iterator -> unit
  val prev : iterator -> unit

  val valid : iterator -> bool

  (** [fill_key it r] places the key for the current binding in the string
    * referred to by [r] if it fits, otherwise it creates a new string and
    * updates the reference.
    * @return length of the key *)
  val fill_key : iterator -> string ref -> int

  (** Similar to {!fill_key}, but returning the value. *)
  val fill_value : iterator -> string ref -> int

  val get_key : iterator -> string
  val get_value : iterator -> string
end

module Snapshot :
sig
  val make : db -> snapshot
  val release : snapshot -> unit

  val get : snapshot -> string -> string option
  val get_exn : snapshot -> string -> string

  val mem : snapshot -> string -> bool

  val iterator : snapshot -> iterator
  val read_access : snapshot -> read_access
end

module Read_access :
sig
  val get : read_access -> string -> string option
  val get_exn : read_access -> string -> string
  val mem : read_access -> string -> bool
  val iterator : read_access -> iterator
end
