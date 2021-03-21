
ocaml-leveldb: OCaml bindings for Google's LevelDB
===================================================
Copyright (c) 2011-2021 Mauricio Fernandez <mfp@acm.org>

These bindings expose nearly the full LevelDB C++ API, including:

* iterators
* snapshots
* batch updates
* support for custom comparators

Blocking functions release the OCaml runtime system, allowing to:

* run them in parallel with other OCaml code
* perform multiple LevelDB operations in parallel

Requirements
------------

* OCaml >= 3.12.0
* GCC with C++ frontend (g++)
* dune to build
* ounit2 for the unit tests
* LevelDB (including dev package libleveldb-dev or similar)
* Snappy (including dev package libsnappy-dev or similar)

Building
--------
Just 

   $ dune build @install

should do. It will build both LevelDB and the OCaml bindings.

You can then install with

   $ dune install

API documentation
-----------------
Refer to src/levelDB.mli.

License
-------
This software is dual-licensed as LGPL+static linking exception and MIT.
Refer to LICENSE.MIT and LICENSE.LGPL+static.
