
type db
type writebatch

exception Error of string

let () =
  Callback.register_exception "org.eigenclass/leveldb/Not_found" Not_found;
  Callback.register_exception "org.eigenclass/leveldb/Error" (Error "")

external open_db :
  string -> write_buffer_size:int -> max_open_files:int ->
  block_size:int  -> block_restart_interval:int -> db = "ldb_open"

let open_db
      ?(write_buffer_size = 4*1024*1024)
      ?(max_open_files = 1000)
      ?(block_size = 4096)
      ?(block_restart_interval = 16) path =
  open_db
    ~write_buffer_size ~max_open_files ~block_size ~block_restart_interval
    path

external close : db -> unit = "ldb_close"

external get_exn : db -> string -> string = "ldb_get"

let get db k = try Some (get_exn db k) with Not_found -> None

external put : db -> string -> string -> sync:bool -> unit = "ldb_put"

let put db ?(sync = false) k v = put db ~sync k v

external delete : db -> string -> sync:bool -> unit = "ldb_put"

let delete db ?(sync = false) k = delete db ~sync k

external mem : db -> string -> bool = "ldb_put"

external iter : (string -> string -> bool) -> db -> unit = "ldb_iter"
external rev_iter : (string -> string -> bool) -> db -> unit = "ldb_rev_iter"

external iter_from :
  (string -> string -> bool) -> db -> string -> unit = "ldb_iter_from"
external rev_iter_from :
  (string -> string -> bool) -> db -> string -> unit = "ldb_rev_iter_from"

external get_approximate_size : db -> string -> string -> Int64.t =
  "ldb_get_approximate_size"

module Batch =
struct
  external make : unit -> writebatch = "ldb_writebatch_make"

  external put :
    writebatch -> string -> string -> unit = "ldb_writebatch_put"  "noalloc"
  external delete :
    writebatch -> string -> unit = "ldb_writebatch_delete"  "noalloc"

  external write : db -> writebatch -> sync:bool -> unit = "ldb_write_batch"
  let write db ?(sync = false) writebatch = write db writebatch ~sync
end
