#!/usr/bin/env python3
import sqlite3
import os
import platform
import json

if platform.system() == "Darwin":
    import pysqlite2.dbapi2 as sqlite3
else:
    import sqlite3

def clean_db(name):
    for f in [name, name + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect(name):
    return sqlite3.connect(f'file:{name}?branches=on')

print("\n=== BEHAVIORAL PROBES ===\n")

# PROBE A: Delete branch with children
print("PROBE A: Delete parent branch that has child branches")
clean_db("probe_a.db")
conn = connect("probe_a.db")
c = conn.cursor()
c.execute("create table t(val text)")
conn.commit()
c.execute("pragma new_branch=parent at master.1")
conn.commit()
c.execute("pragma new_branch=child at parent.2")
conn.commit()
c.execute("pragma branch=master")
try:
    c.execute("pragma del_branch(parent)")
    print("  RESULT: Delete succeeded")
    c.execute("pragma branch=child")
    c.execute("select count(*) from t")
    result = c.fetchone()[0]
    print(f"  Child branch still works: {result >= 0}")
except Exception as e:
    print(f"  RESULT: Error - {e}")
conn.close()
clean_db("probe_a.db")

# PROBE B: Branch log with multiple statements in one commit
print("\nPROBE B: branch_log with multi-statement single commit")
clean_db("probe_b.db")
conn = connect("probe_b.db")
c = conn.cursor()
c.execute("pragma branch_log")
if c.fetchone()[0] == "disabled":
    print("  RESULT: branch_log disabled in this build")
else:
    c.execute("create table t(id integer, val text)")
    c.execute("insert into t values (1, 'a')")
    c.execute("insert into t values (2, 'b')")
    c.execute("insert into t values (3, 'c')")
    conn.commit()
    c.execute("pragma branch_log")
    rows = c.fetchall()
    print(f"  Number of log rows: {len(rows)}")
    for row in rows:
        print(f"    Branch: {row[0]}, Commit: {row[1]}, SQL: {row[2][:50]}")
conn.close()
clean_db("probe_b.db")

# PROBE C: Max branch name length
print("\nPROBE C: Max branch name length before error")
clean_db("probe_c.db")
conn = connect("probe_c.db")
c = conn.cursor()
c.execute("create table t(val text)")
conn.commit()

for length in [50, 100, 200, 256, 512, 1024, 2048]:
    name = "b" * length
    try:
        c.execute(f"pragma new_branch={name} at master.1")
        c.execute("pragma branch=master")
        print(f"  Length {length}: SUCCESS")
        break
    except Exception as e:
        print(f"  Length {length}: FAILED - {str(e)[:60]}")

conn.close()
clean_db("probe_c.db")

# PROBE D: new_branch without "at source.N" specification
print("\nPROBE D: pragma new_branch=x (no 'at source.N')")
clean_db("probe_d.db")
conn = connect("probe_d.db")
c = conn.cursor()
c.execute("create table t(val text)")
conn.commit()
c.execute("insert into t values ('row1')")
conn.commit()
c.execute("insert into t values ('row2')")
conn.commit()
c.execute("pragma new_branch=child at master.3")
c.execute("pragma branch_info(child)")
info = json.loads(c.fetchone()[0])
print(f"  Source branch: {info['source_branch']}")
print(f"  Source commit: {info['source_commit']}")
print(f"  Expected: source_branch=master, source_commit=3")
conn.close()
clean_db("probe_d.db")

# PROBE E: Rename current branch
print("\nPROBE E: Can you rename the current branch?")
clean_db("probe_e.db")
conn = connect("probe_e.db")
c = conn.cursor()
c.execute("create table t(val text)")
conn.commit()
c.execute("pragma branch")
current = c.fetchone()[0]
print(f"  Current branch: {current}")
try:
    c.execute(f"pragma rename_branch {current} renamed")
    c.execute("pragma branch")
    after = c.fetchone()[0]
    print(f"  After rename attempt: {after}")
    print(f"  RESULT: Rename succeeded, now on '{after}'")
except Exception as e:
    print(f"  RESULT: Error - {str(e)[:60]}")
conn.close()
clean_db("probe_e.db")

# PROBE F: branch_tree for 3-level deep tree
print("\nPROBE F: branch_tree for 3-level deep tree")
clean_db("probe_f.db")
conn = connect("probe_f.db")
c = conn.cursor()
c.execute("create table t(val text)")
conn.commit()
c.execute("pragma new_branch=level1 at master.1")
conn.commit()
c.execute("pragma new_branch=level2 at level1.2")
conn.commit()
c.execute("pragma new_branch=level3 at level2.3")
conn.commit()
c.execute("pragma branch_tree")
tree = c.fetchone()[0]
print("  Tree structure:")
for line in tree.split('\n'):
    print(f"    {line}")
conn.close()
clean_db("probe_f.db")

print("\n=== END OF PROBES ===\n")
