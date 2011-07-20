
open Printf

module LDB = Leveldb

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

let loop_cost n =
  let r = FRAND.make () in
  let t0 = Unix.gettimeofday () in
    for i = 1 to n do
      let k = FRAND.int r in
        ignore (string_of_int k);
        ignore (string_of_int i)
    done;
    (Unix.gettimeofday () -. t0)

let () =
  let db = LDB.open_db "/tmp/ldb" in
  let r = FRAND.make () in
  let n = 1_000_000 in

  let t0 = Unix.gettimeofday () in
    for i = 1 to n do
      let k = FRAND.int r in
        LDB.put db (string_of_int k) (string_of_int i)
    done;
    LDB.close db;
    let dt = Unix.gettimeofday () -. t0 in
    let dt0 = loop_cost n in
      printf "Needed %8.5fs (empty: %8.5fs)\n" dt dt0

