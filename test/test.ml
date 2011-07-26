
open Printf
open OUnit
open Test_00util

module L = LevelDB
module I = LevelDB.Iterator
module B = LevelDB.Batch

let aeq_iterator_bindings ?(next = L.Iterator.next) expected it =
  let l = ref [] in
    while I.valid it do
      l := (I.get_key it, I.get_value it) :: !l;
      next it;
    done;
    aeq_list (fun (k, v) -> sprintf "%S:%S" k v) expected (List.rev !l)

let aeq_value = aeq_some ~msg:"Wrong value" (sprintf "%S")

let assert_found db key =
  aeq_bool ~msg:(sprintf "mem %S" key) true (L.mem db key)

let assert_not_found db key =
  aeq_bool ~msg:(sprintf "mem %S" key) false (L.mem db key);
  assert_not_found (fun () -> ignore (L.get_exn db key))

module TestBasic =
struct
  let test_put_get db =
    aeq_none (L.get db "test_put_get");
    assert_not_found db "test_put_get";
    L.put db "test_put_get" "1";
    L.put db "test_put_get" "2";
    aeq_value "2" (L.get db "test_put_get");
    aeq_string "2" (L.get_exn db "test_put_get");
    assert_found db "test_put_get"

  let test_delete db =
    L.delete db "test_delete";
    assert_not_found db "test_delete";
    L.put db "test_delete" "x";
    aeq_value "x" (L.get db "test_delete");
    assert_found db "test_delete";
    L.delete db "test_delete";
    assert_not_found db "test_delete"

  let tests =
    [
      "put/get/mem", test_put_get;
      "delete/mem", test_delete;
    ]
end

module TestSnapshot =
struct
  module S = L.Snapshot

  let test_isolation db =
    let s = S.make db in
      L.put db "test_isolation" "bar";
      aeq_none ~msg:"Should not find data in isolated snapshot"
        (S.get s "test_isolation");
      aeq_bool false (S.mem s "test_isolation");
      S.release s;
      L.put db "test_isolation" "1";
      let s = S.make db in
        L.put db "test_isolation" "2";
        aeq_some (sprintf "%S") "1" (S.get s "test_isolation");
        aeq_bool ~msg:"Should find data" true (S.mem s "test_isolation");
        aeq_bool true (S.mem s "test_isolation")

  let test_iterator db =
    let vector = List.map (fun k -> (k, k ^ k)) [ "a"; "b"; "c"; "x"; "w" ] in
      List.iter (fun (k, v) -> L.put db k v) vector;
      let s = S.make db in
      let it = S.iterator s in
        I.seek_to_first it;
        aeq_iterator_bindings (List.sort compare vector) it;
        List.iter (fun (k, v) -> L.put db k v) ["a", "1"; "f", "2"];
        I.seek_to_first it;
        aeq_iterator_bindings (List.sort compare vector) it;
        let s = S.make db in
        let it  = S.iterator s in
          I.seek_to_first it;
          aeq_iterator_bindings
            ["a", "1"; "b", "bb"; "c", "cc"; "f", "2"; "w", "ww"; "x", "xx"]
            it

  let test_put_and_snapshot db =
    let s = L.put_and_snapshot db "test_put_and_snapshot" "1" in
      L.put db "test_put_and_snapshot" "2";
      aeq_value "1" (S.get s "test_put_and_snapshot");
      aeq_value "2" (L.get db "test_put_and_snapshot");
      S.release s

  let test_delete_and_snapshot db =
    L.put db "test_delete_and_snapshot" "1";
    let s = L.delete_and_snapshot db "test_put_and_snapshot" in
      L.put db "test_delete_and_snapshot" "2";
      aeq_none ~msg:"No value should be found in snapshot"
        (S.get s "test_put_and_snapshot");
      S.release s

  let test_write_and_snapshot db =
    let b = B.make () in
      B.put b "a" "1";
      B.put b "a" "2";
      B.put b "b" "1";
      let s = B.write_and_snapshot db b in
        L.delete db "b";
        L.put db "a" "3";
        aeq_value "2" (S.get s "a");
        aeq_value "1" (S.get s "b");
        S.release s

  let test_db_closed_before_release db =
    let s = S.make db in
      L.close db;
      S.release s

  let test_release_when_iterator_in_use db =
    L.put db "a" "a";
    L.put db "b" "b";
    let s = S.make db in
    let it = S.iterator s in
      L.put db "c" "c";
      I.seek_to_first it;
      aeq_bool true (I.valid it);
      aeq_string ~msg:"Value" "a" (I.get_value it);
      S.release s;
      I.next it;
      aeq_bool ~msg:"Should still be valid" true (I.valid it);
      aeq_string ~msg:"Value for 'b'" "b" (I.get_value it);
      I.next it;
      aeq_bool ~msg:"Should reach EOS" false (I.valid it)

  let tests =
    [
      "isolation", test_isolation;
      "iterator", test_iterator;
      "put_and_snapshot", test_put_and_snapshot;
      "delete_and_snapshot", test_delete_and_snapshot;
      "write_and_snapshot", test_write_and_snapshot;
      "DB closed before snapshot release", test_db_closed_before_release;
      "DB closed with snapshot iterator in use", test_release_when_iterator_in_use;
    ]
end

let with_db f () =
  let dir = make_temp_dir () in
  let db = L.open_db dir in
    try
      f db
    with e -> L.close db; raise e

let test_with_db (name, f) = name >:: with_db f

let tests =
  "All" >:::
  [
    "Basic" >::: List.map test_with_db TestBasic.tests;
    "Snapshot" >::: List.map test_with_db TestSnapshot.tests;
  ]

let () =
  ignore (run_test_tt_main tests)
