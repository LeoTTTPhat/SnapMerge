"""SnapMerge: sound three-way merging of diverged Apache Iceberg table branches.

Research prototype. Modules:
  expr       filter AST shared by writer, rules, oracle (Iceberg / Arrow / SQL backends)
  catalog    PyIceberg 0.12.0 SqlCatalog helpers and the one module that touches PyIceberg internals
  writer     logical-operation writer (records op metadata in snapshot summaries)
  lineage    LCA and per-side operation / physical-delta reconstruction
  rules      commutativity rules and the physical composition plan
  emit       merge-snapshot emission (manifests, sequence numbers, atomic commit)
  merge      SnapMerge entry point (lineage -> rules -> emit)
  baselines  replay, lakeFS, Nessie, fast-forward, cherry-pick, row-level merge
  oracle     independent DuckDB serial-order oracle and two-reader checks
  sources    data sources (TPC-H via DuckDB dbgen, NYC TLC yellow taxi)
  scenarios  seeded scenario generator (7 mixes)
"""
__version__ = "0.1.0"
