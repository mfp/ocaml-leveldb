
type db
type writebatch
type iterator
type snapshot

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

external put : db -> string -> string -> sync:bool -> snapshot:bool ->
  snapshot option = "ldb_put"

external delete : db -> string -> sync:bool -> snapshot:bool ->
  snapshot option = "ldb_delete"

let delete_and_snapshot db ?(sync = false) k =
  match delete db ~sync ~snapshot:true k with None -> assert false | Some s -> s

let delete db ?(sync = false) k = ignore (delete db ~sync ~snapshot:false k)

let put_and_snapshot db ?(sync = false) k v =
  match put db ~sync ~snapshot:true k v with None -> assert false | Some s -> s

let put db ?(sync = false) k v = ignore (put db ~sync ~snapshot:false k v)

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

  external write : db -> writebatch -> sync:bool -> snapshot:bool ->
    snapshot option = "ldb_write_batch"

  let write_and_snapshot db ?(sync = false) writebatch =
    match write db writebatch ~sync ~snapshot:true with
        None -> assert false
      | Some s -> s

  let write db ?(sync = false) writebatch =
    ignore (write db writebatch ~sync ~snapshot:false)
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

  let fill_and_resize_if_needed name f it buf =
    let len = f it !buf in
      if len <= String.length !buf then len
      else begin
        if len > Sys.max_string_length then
          error "Iterator.%s: string is larger than Sys.max_string_length" name;
        buf := String.create len;
        f it !buf
      end

  let fill_key it buf = fill_and_resize_if_needed "fill_key" key_unsafe it buf
  let fill_value it buf = fill_and_resize_if_needed "fill_value" value_unsafe it buf

  let get_key it = let b = ref "" in ignore (fill_key it b); !b
  let get_value it = let b = ref "" in ignore (fill_value it b); !b
end

module Snapshot =
struct
  external make : db -> snapshot = "ldb_snapshot_make"
  external release : snapshot -> unit = "ldb_snapshot_release"
  external get_exn : snapshot -> string -> string = "ldb_snapshot_get"

  let get t k = try Some (get_exn t k) with Not_found -> None

  external mem : snapshot -> string -> bool = "ldb_snapshot_mem"
  external iterator : snapshot -> iterator = "ldb_snapshot_make_iterator"
end
