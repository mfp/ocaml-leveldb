open Printf
open OUnit
open Test_utils

module L = LevelDB
module I = LevelDB.Iterator
module B = LevelDB.Batch

let string_of_binding (k, v) = sprintf "%S:%S" k v

let aeq_bindings ?msg expected actual =
  aeq_list ?msg string_of_binding expected actual

let aeq_iterator_bindings ?msg ?(next = L.Iterator.next) expected it =
  let l = ref [] in
    while I.valid it do
      l := (I.get_key it, I.get_value it) :: !l;
      next it;
    done;
    aeq_bindings ?msg expected (List.rev !l)

let aeq_value = aeq_some ~msg:"Wrong value" (sprintf "%S")

let assert_found db key =
  aeq_bool ~msg:(sprintf "mem %S" key) true (L.mem db key)

let assert_not_found db key =
  aeq_bool ~msg:(sprintf "mem %S" key) false (L.mem db key);
  assert_not_found (fun () -> ignore (L.get_exn db key))

let assert_raises_error ?(msg = "") f =
  try
    ignore (f ());
    assert_failure (sprintf "Should have raised an error. %s" msg)
  with L.Error _ -> ()

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

  let test_iter db =
    let vector = [ "a", "1"; "b", "2"; "c", "3" ] in
      List.iter (fun (k, v) -> L.put db k v) vector;
      let l = ref [] in
        L.iter (fun k v -> l := (k, v) :: !l; true) db;
        aeq_bindings vector (List.rev !l);
        l := [];
        L.iter (fun k v -> l:= (k, v) :: !l; false) db;
        aeq_bindings ["a", "1"] (List.rev !l)

  let test_iter_from db =
    let vector = [ "a", "1"; "b", "2"; "c", "3" ] in
      List.iter (fun (k, v) -> L.put db k v) vector;
      let l = ref [] in
        L.iter_from (fun k v -> l := (k, v) :: !l; true) db "b";
        aeq_bindings ["b", "2"; "c", "3"] (List.rev !l);
        l := [];
        L.iter_from (fun k v -> l:= (k, v) :: !l; false) db "b";
        aeq_bindings ["b", "2"] (List.rev !l)

  let test_rev_iter db =
    let vector = [ "a", "1"; "b", "2"; "c", "3" ] in
      List.iter (fun (k, v) -> L.put db k v) vector;
      let l = ref [] in
        L.rev_iter (fun k v -> l := (k, v) :: !l; true) db;
        aeq_bindings vector !l;
        l := [];
        L.rev_iter (fun k v -> l:= (k, v) :: !l; false) db;
        aeq_bindings ["c", "3"] !l

  let test_rev_iter_from db =
    let vector = [ "a", "1"; "b", "2"; "c", "3" ] in
      List.iter (fun (k, v) -> L.put db k v) vector;
      let l = ref [] in
        L.rev_iter_from (fun k v -> l := (k, v) :: !l; true) db "b";
        aeq_bindings ["a", "1"; "b", "2"] !l;
        l := [];
        L.rev_iter_from (fun k v -> l:= (k, v) :: !l; false) db "b";
        aeq_bindings ["b", "2"] !l

  let test_iter_stability db =
    let aeq ?msg expected it =
      I.seek_to_first it;
      aeq_iterator_bindings ?msg expected it in

    let it1 = I.make db in
      aeq ~msg:"it1 before put" [] it1;
      List.iter (fun (k, v) -> L.put db k v) [ "a", "aa"; "b", "bb" ];
      let it2 = I.make db in
        aeq ~msg:"it1 after put" [] it1;
        aeq ~msg:"it2 after put" [ "a", "aa"; "b", "bb" ] it2;
        L.delete db "a";
        let it3 = I.make db in
          aeq ~msg:"it1 after del" [] it1;
          aeq ~msg:"it2 after del" [ "a", "aa"; "b", "bb" ] it2;
          aeq ~msg:"it3 after del" [ "b", "bb" ] it3

  let test_db_close_with_iter_in_use db =
    List.iter (fun (k, v) -> L.put db k v) [ "a", "aa"; "b", "bb" ];
    let it = I.make db in
      I.seek_to_first it;
      L.close db;
      aeq_bool ~msg:"Iterator.valid for iterator whose db was closed" false (I.valid it);
      assert_raises_error
        ~msg:"should raise an Error when using a closed iterator"
        (fun () -> ignore (I.get_key it))

  let tests =
    [
      "put/get/mem", test_put_get;
      "delete/mem", test_delete;
      "iter", test_iter;
      "rev_iter", test_rev_iter;
      "iter_from", test_iter_from;
      "rev_iter_from", test_rev_iter_from;
      "iterator stability", test_iter_stability;
      "DB closed when iterator in use", test_db_close_with_iter_in_use;
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

  let test_batch_ops db =
    List.iter (fun (k, v) -> L.put db k v)
      [ "a", "1"; "b", "2" ];
    let b = B.make () in
      B.put b "a" "x";
      B.delete b "b";
      assert_raises_error (fun () -> B.put_substring b "a" (-1) 1 "z" 0 1);
      assert_raises_error (fun () -> B.put_substring b "a" 1 1 "z" 0 1);
      assert_raises_error (fun () -> B.put_substring b "a" 0 2 "z" 0 1);
      assert_raises_error (fun () -> B.put_substring b "a" (-1) 2 "z" 0 1);
      assert_raises_error (fun () -> B.put_substring b "z" 0 1 "a" (-1) 1);
      assert_raises_error (fun () -> B.put_substring b "z" 0 1 "a" 1 1);
      assert_raises_error (fun () -> B.put_substring b "z" 0 1 "a" 0 2);
      assert_raises_error (fun () -> B.put_substring b "z" 0 1 "a" (-1) 2);
      B.write db b;
      aeq_value "x" (L.get db "a");
      assert_not_found db "b"


  let tests =
    [
      "isolation", test_isolation;
      "iterator", test_iterator;
      "DB closed before snapshot release", test_db_closed_before_release;
      "snapshot released when iterator in use", test_release_when_iterator_in_use;
      "batch operations", test_batch_ops;
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
