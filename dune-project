(lang dune 2.0)

(name leveldb)

(license MIT)

(authors "<mfp@acm.org>")

(maintainers "<mfp@acm.org>")

(source
 (github mfp/ocaml-leveldb))

(generate_opam_files true)

(package
 (name leveldb)
 (synopsis "OCaml bindings for Google's LevelDB library")
 (description
  "These bindings expose nearly the full LevelDB C++ API, including: iterators, snapshots, batch updates and support for custom comparators. Blocking functions release the OCaml runtime system, allowing to run them in parallel with other OCaml code and to perform multiple LevelDB operations in parallel.")
 (depends
  (ocaml
   (>= 4.06))
  (dune
   (>= 2.0))
  (ounit2 :with-test)
  (odoc :with-doc)
  conf-leveldb))
