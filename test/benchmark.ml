
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
         for i = 1 to n do
           let k = FRAND.int r in
             ignore (string_of_int k);
         done)

let bm_get db ?seed n =
  let r = FRAND.make ?seed () in
  let dt =
    time
      (fun () ->
         for i = 1 to n do
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
         let v = ref "" in
           for i = 1 to n do
             let k = FRAND.int r in
             let key = string_of_int k in
               LDB.Iterator.seek it key 0 (String.length key);
               ignore (LDB.Iterator.value it v)
             done;
           LDB.Iterator.close it)
  in dt -. loop_k_cost n

let bm_put db ?seed n =
  let r = FRAND.make () in
  let dt =
    time
      (fun () ->
         for i = 1 to n do
           let k = FRAND.int r in
             LDB.put db (string_of_int k) (string_of_int i)
         done)
  in dt -. loop_kv_cost n

let () =
  let n = 1_000_000 in
  let seed = 0 in

    List.iter
      (fun (op, f) ->
         let db = LDB.open_db "/tmp/ldb" in
         let dt = f db ?seed:(Some seed) n in
           printf "%20s  %7d/s\n%!" op (truncate (float n /. dt));
           LDB.close db)
      [
        "put", bm_put;
        "get", bm_get;
        "Iterator.value", bm_iter_value;
      ]

