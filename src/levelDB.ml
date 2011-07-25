
type db
type writebatch
type iterator

exception Error of string

let () =
  Callback.register_exception "org.eigenclass/leveldb/Not_found" Not_found;
  Callback.register_exception "org.eigenclass/leveldb/Error" (Error "")

let error fmt =
  Printf.ksprintf (fun s -> raise (Error s)) fmt

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

external destroy : string -> bool = "ldb_destroy"
external repair : string -> bool = "ldb_repair"

external close : db -> unit = "ldb_close"

external get_exn : db -> string -> string = "ldb_get"

let get db k = try Some (get_exn db k) with Not_found -> None

external put : db -> string -> string -> sync:bool -> unit = "ldb_put"

let put db ?(sync = false) k v = put db ~sync k v

external delete : db -> string -> sync:bool -> unit = "ldb_delete"

let delete db ?(sync = false) k = delete db ~sync k

external mem : db -> string -> bool = "ldb_mem"

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

module Iterator =
struct
  external make : db -> iterator = "ldb_make_iter"
  external close : iterator -> unit = "ldb_iter_close"
  external seek_to_first : iterator -> unit = "ldb_it_first"
  external seek_to_last : iterator -> unit = "ldb_it_last"

  external seek_unsafe : iterator -> string -> int -> int -> unit =
    "ldb_it_seek_unsafe"

  let seek it s off len =
    if off < 0 || len < 0 || off + len > String.length s then
      error "Iterator.seek: invalid arguments (key:%S off:%d len:%d)"
        s off len;
    seek_unsafe it s off len

  external next : iterator -> unit = "ldb_it_next"
  external prev : iterator -> unit = "ldb_it_prev"

  external valid : iterator -> bool = "ldb_it_valid" "noalloc"

  external key_unsafe : iterator -> string -> int = "ldb_it_key_unsafe"
  external value_unsafe : iterator -> string -> int = "ldb_it_value_unsafe"

  let get_and_resize_buf_if_needed name f it buf =
    let len = f it !buf in
      if len <= String.length !buf then len
      else begin
        if len > Sys.max_string_length then
          error "Iterator.%s: string is larger than Sys.max_string_length" name;
        buf := String.create len;
        f it !buf
      end

  let key it buf = get_and_resize_buf_if_needed "key" key_unsafe it buf
  let value it buf = get_and_resize_buf_if_needed "value" value_unsafe it buf
end
