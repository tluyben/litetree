# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## What this project is

LiteTree is a fork of SQLite (`sqlite3.c` amalgamation) that adds git-like branching. The entire implementation lives in a single modified `sqlite3.c` file (~18k lines of upstream SQLite + ~2k lines of LiteTree additions). Pages are stored in LMDB instead of a flat file; branches share pages via copy-on-write at commit time.

The library is a drop-in replacement for `libsqlite3` — it exposes the same C API and is loaded by Python's `sqlite3` module via `LD_LIBRARY_PATH`.

## Build

```bash
make                  # builds liblitetree.so.0.0.1 and the sqlite3 shell
make debug            # adds -g -DSQLITE_DEBUG=1 -DDEBUGPRINT
make clean
```

Dependencies: LMDB headers and library. Override paths if non-standard:
```bash
make LMDBPATH=/path/to/lib LMDBINCPATH=/path/to/include
```

The library is built from `sqlite3.c` only — there is no configure step.

## Tests

```bash
make test                                        # run both test suites
cd test && LD_LIBRARY_PATH=.. python3 test.py -v                        # main suite
cd test && LD_LIBRARY_PATH=.. python3 test-64bit-commit-ids.py -v       # 64-bit commit ID suite
```

Run a single test by name:
```bash
cd test && LD_LIBRARY_PATH=.. python3 -m unittest test.TestSQLiteBranches.test01_branches -v
```

Tests live in `test/` and use Python `unittest`. The suite checks the SQLite version at startup and exits if it doesn't match `sqlite_version` at the top of `test.py` — update that constant when upgrading SQLite.

Databases created by tests are `.db` + `.db-lock` (the LMDB lock file) pairs, cleaned up at test start.

## Code architecture

All LiteTree additions are in `sqlite3.c`. The key sections:

- **`branches.h`** (inlined near the bottom of `sqlite3.c`): defines `branch_info` struct and the global `litetree_state` struct that holds the branch array, LMDB env, and connection state.
- **Branch pragma dispatch**: LiteTree adds cases to SQLite's `pragma.c` switch block. When upgrading SQLite, these cases must remain at the correct nesting level — a past upgrade accidentally nested them inside `PragTyp_INTEGRITY_CHECK`, making them unreachable.
- **`sqlite3PagerDirectReadOk()`**: Returns 0 when `pPager->useBranches` is set. This is the page overflow fix — branch pages live in LMDB, not the file, so direct-read bypassing the pager cache must be disabled.
- **`sqlite3BranchReadPage()`**: Reads a page from LMDB for the current branch/commit. Called from the pager read path.
- **`litetreeCheckExecCommand()`**: Tracks SQL commands per commit for `branch_log`. Must be placed in `sqlite3Step()`, not `sqlite3VtabImportErrmsg()` — a past placement mistake broke log tracking.
- **`process_branch_uri()`**: Parses `?branches=on` and `?single_connection=true` URI parameters.

Branches are opened via URI: `file:data.db?branches=on`. Without this parameter the database behaves as normal SQLite.

## Key pragma commands (tested)

```sql
PRAGMA branch                         -- current branch name
PRAGMA branch=<name>                  -- switch branch
PRAGMA branch=<name>.<commit>         -- read-only at specific commit
PRAGMA branches                       -- list all branches
PRAGMA new_branch=<name> at <src>.<n> -- create branch from commit
PRAGMA del_branch(<name>)
PRAGMA rename_branch <old> <new>
PRAGMA branch_truncate(<name>.<n>)
PRAGMA branch_info(<name>)
PRAGMA branch_log(<name>)
PRAGMA branch_tree
PRAGMA branch_merge --forward <parent> <child> <num_commits>
```

## Known constraints

- Max 1024 branches (hardcoded in `branches.h`).
- Savepoints are not supported (the pragma is accepted but does nothing useful).
- `branch_diff` and `branch_rebase` pragmas are defined but not fully implemented.
- LMDB databases are architecture-specific (no cross-arch portability).
- The rename_branch pragma entry must stay in alphabetical order in the pragma dispatch table or `pragmaLocate()` won't find it.
- Writes to a historical commit (`PRAGMA branch=name.N`) are silently accepted by `execute()` but raise "access permission denied" at `commit()` — the pager cannot detect the read-only state until flush time.
- `rename_branch` raises an error if the target name already exists (previously this silently created corrupt duplicate-name state).
- `branch_truncate` to a commit beyond the branch head raises an error (previously a silent no-op).

## SQLite upgrade notes

When merging a new SQLite amalgamation:

1. Re-apply the `sqlite3PagerDirectReadOk()` guard (`if( pPager->useBranches ) return 0;`).
2. Verify pragma dispatch nesting hasn't changed — LiteTree cases must be at the top-level `switch` on `PragTyp_*`, not inside another case's block.
3. Check that `litetreeCheckExecCommand()` is still called from `sqlite3Step()`.
4. Verify `rename_branch` is in alphabetical position in the pragma table.
5. Update `sqlite_version` in `test/test.py`.
