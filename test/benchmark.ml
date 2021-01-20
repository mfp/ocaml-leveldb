open Printf

module LDB = LevelDB

module FRAND =
struct
  type t = { mutable s0 : int; mutable s1 : int }

  let make ?(seed = 0) () = let s = seed in
    { s0 = if s = 0 then 521288629 else s; s1 = if s = 0 then 362436069 else s }

  let int t = let s0 = t.s0 and s1 = t.s1 in
    t.s0 <- (18000 * (s0 land 0xFFFF) + (s0 lsr 16)) land 0x3FFFFFFF;
    t.s1 <- (30903 * (s1 land 0xFFFF) + (s1 lsr 16)) land 0x3FFFFFFF;
    ((t.s0 lsl 16) + (t.s1 land 0xFFFF)) land 0x3FFFFFFF
end

let time f =
  let t0 = Unix.gettimeofday () in
    f ();
    Unix.gettimeofday () -. t0

let loop_kv_cost n =
  let r = FRAND.make () in
    time
      (fun () ->
         for i = 1 to n do
           let k = FRAND.int r in
             ignore (string_of_int k);
             ignore (string_of_int i)
         done)

let loop_k_cost n =
  let r = FRAND.make () in
    time
      (fun () ->
         for _i = 1 to n do
           let k = FRAND.int r in
             ignore (string_of_int k);
         done)

let bm_get db ?seed n =
  let r = FRAND.make ?seed () in
  let dt =
    time
      (fun () ->
         for _i = 1 to n do
           let k = FRAND.int r in
             ignore (LDB.get db (string_of_int k))
         done)
  in dt -. loop_k_cost n

let bm_iter_value db ?seed n =
  let r = FRAND.make ?seed () in
  let dt =
    time
      (fun () ->
         let it = LDB.Iterator.make db in
         let v = ref Bytes.empty in
           for _i = 1 to n do
             let k = FRAND.int r in
             let key = string_of_int k in
               LDB.Iterator.seek it key 0 (String.length key);
               ignore (LDB.Iterator.fill_value it v)
             done;
           LDB.Iterator.close it)
  in dt -. loop_k_cost n

let bm_put_aux ~sync db ?seed:_ n =
  let r = FRAND.make () in
  let dt =
    time
      (fun () ->
         for i = 1 to n do
           let k = FRAND.int r in
             LDB.put ~sync db (string_of_int k) (string_of_int i)
         done)
  in dt -. loop_kv_cost n

let bm_put = bm_put_aux ~sync:false
let bm_put_sync = bm_put_aux ~sync:true

let bm_batch_put_sync db ?seed:_ n =
  let r = FRAND.make () in
  let dt =
    time
      (fun () ->
         for i = 1 to n / 1000 do
           let b = LDB.Batch.make () in
           let m = i * 1000 in
             for j = 0 to 1000 do
               let i = m + j in
               let k = FRAND.int r in
                 LDB.Batch.put b (string_of_int k) (string_of_int i)
             done;
             LDB.Batch.write db ~sync:true b
         done)
  in dt -. loop_kv_cost n

let bm_iter_scan_aux init next db ?seed:_ _n =
  let dt =
    time
      (fun () ->
         let it = LDB.Iterator.make db in
         let k = ref Bytes.empty in
         let v = ref Bytes.empty in
           init it;
           while LDB.Iterator.valid it do
             ignore (LDB.Iterator.fill_key it k);
             ignore (LDB.Iterator.fill_value it v);
             next it;
           done)
  in dt

let bm_iter_seq_scan =
  bm_iter_scan_aux LDB.Iterator.seek_to_first LDB.Iterator.next

let bm_iter_rev_scan =
  bm_iter_scan_aux LDB.Iterator.seek_to_last LDB.Iterator.prev

let downscale factor f db ?seed n =
  f db ?seed (n / factor) *. float factor

let () =
  let n = 1_000_000 in
  let seed = 0 in

  let compact () =
    print_endline "Compacting...";
    let db = LDB.open_db ~cache_size:10 "/tmp/ldb" in
      LDB.compact_range db ~from_key:(Some "") ~to_key:None;
      LDB.close db;
      print_endline "DONE" in

  let print_stats () =
    let db = LDB.open_db ~cache_size:10 "/tmp/ldb" in
      begin match LDB.get_property db "leveldb.stats" with
          None -> ()
        | Some x -> print_newline (); print_endline x
      end;
      LDB.close db

  in

    List.iter
      (function
           `O (op, f) ->
             let db = LDB.open_db "/tmp/ldb" in
             let dt = f db ?seed:(Some seed) n in
               printf "%20s  %7d/s\n%!" op (truncate (float n /. dt));
               LDB.close db
         | `Clear -> ignore (LDB.destroy "/tmp/ldb")
         | `E f -> f ())
      [
        `Clear;
        `O ("put", bm_put);
        `O ("get", bm_get);
        `O ("Iterator.value", bm_iter_value);
        `E compact;
        `O ("get", bm_get);
        `O ("Iterator.value", bm_iter_value);
        `Clear;
        `O ("sync put", downscale 50 bm_put_sync);
        `Clear;
        `O ("batch put", bm_batch_put_sync);
        `O ("seq scan", bm_iter_seq_scan);
        `O ("rev scan", bm_iter_rev_scan);
        `E compact;
        `O ("seq scan", bm_iter_seq_scan);
        `O ("rev scan", bm_iter_rev_scan);
        `E print_stats;
      ]
