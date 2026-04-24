#
# Copyright defined in LICENSE.txt
#
# SQLite compatibility regression tests.
# Each test directly encodes a breakage that occurred during a past SQLite upgrade
# so that future upgrades catch the same regressions automatically.
#
import unittest
import json
import os
import platform

if platform.system() == "Darwin":
    import pysqlite2.dbapi2 as sqlite3
else:
    import sqlite3

sqlite_version = "3.53.0"

if sqlite3.sqlite_version != sqlite_version:
    print("wrong SQLite version. expected: " + sqlite_version + " found: " + sqlite3.sqlite_version)
    import sys
    sys.exit(1)

DB = "compat_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')


class TestSQLiteCompat(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    # --- RETURNING clause ---

    def test01_returning_insert_across_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer primary key, val text)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("insert into t(val) values ('x') returning id, val")
        row = c.fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(row[1], "x")
        conn.commit()
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test02_returning_update_across_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer primary key, val integer)")
        conn.commit()
        c.execute("insert into t values (1, 10)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set val=val*2 where id=1 returning id, val")
        row = c.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 20)
        conn.commit()
        c.execute("pragma branch=master")
        c.execute("select val from t where id=1")
        self.assertEqual(c.fetchone()[0], 10)
        conn.close()

    def test03_returning_delete_across_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer primary key, val text)")
        conn.commit()
        c.execute("insert into t values (1, 'to_delete')")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("delete from t where id=1 returning id, val")
        row = c.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], "to_delete")
        conn.commit()
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()

    def test04_returning_insert_with_overflow(self):
        """RETURNING works when the inserted value requires overflow pages"""
        conn = connect()
        c = conn.cursor()
        big = "Y" * 50000
        c.execute("create table t(id integer primary key, data text)")
        conn.commit()
        c.execute("insert into t(data) values (?) returning id", (big,))
        row = c.fetchone()
        self.assertIsNotNone(row[0])
        conn.commit()
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    # --- ALTER TABLE ---

    def test05_alter_drop_column_with_overflow_data(self):
        """DROP COLUMN works when other columns hold overflow-length data"""
        conn = connect()
        c = conn.cursor()
        big = "Z" * 50000
        c.execute("create table t(id integer, keep_col text, drop_col text)")
        conn.commit()
        c.execute("insert into t values (1, 'keep_me', ?)", (big,))
        conn.commit()
        c.execute("alter table t drop column drop_col")
        conn.commit()
        c.execute("pragma table_info(t)")
        cols = [row[1] for row in c.fetchall()]
        self.assertNotIn("drop_col", cols)
        self.assertIn("keep_col", cols)
        c.execute("select keep_col from t where id=1")
        self.assertEqual(c.fetchone()[0], "keep_me")
        conn.close()

    def test06_alter_add_column_with_default_isolated(self):
        """ADD COLUMN with DEFAULT is branch-isolated"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer)")
        conn.commit()
        c.execute("insert into t values (1)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("alter table t add column extra text default 'default_val'")
        conn.commit()
        c.execute("select extra from t where id=1")
        self.assertEqual(c.fetchone()[0], "default_val")
        c.execute("pragma branch=master")
        c.execute("pragma table_info(t)")
        cols = [row[1] for row in c.fetchall()]
        self.assertNotIn("extra", cols)
        conn.close()

    # --- JSON functions ---

    def test07_json_functions_across_branch_switch(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(data text)")
        conn.commit()
        c.execute("insert into t values (?)", ('{"key": "master_val"}',))
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set data=?", ('{"key": "branch_val"}',))
        conn.commit()
        c.execute("select json_extract(data, '$.key') from t")
        self.assertEqual(c.fetchone()[0], "branch_val")
        c.execute("pragma branch=master")
        c.execute("select json_extract(data, '$.key') from t")
        self.assertEqual(c.fetchone()[0], "master_val")
        conn.close()

    # --- CTE ---

    def test08_cte_across_branch_switch(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(n integer)")
        conn.commit()
        for i in range(1, 6):
            c.execute("insert into t values (?)", (i,))
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("delete from t where n > 3")
        conn.commit()
        c.execute("with s as (select sum(n) as total from t) select total from s")
        self.assertEqual(c.fetchone()[0], 6)  # 1+2+3
        c.execute("pragma branch=master")
        c.execute("with s as (select sum(n) as total from t) select total from s")
        self.assertEqual(c.fetchone()[0], 15)  # 1+2+3+4+5
        conn.close()

    # --- Window functions ---

    def test09_window_functions_across_branch_switch(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, val integer)")
        conn.commit()
        for i in range(1, 5):
            c.execute("insert into t values (?, ?)", (i, i * 10))
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("insert into t values (5, 50)")
        conn.commit()
        c.execute("select id, sum(val) over (order by id) from t order by id")
        rows = c.fetchall()
        self.assertEqual(rows[-1][1], 150)  # 10+20+30+40+50
        c.execute("pragma branch=master")
        c.execute("select id, sum(val) over (order by id) from t order by id")
        rows = c.fetchall()
        self.assertEqual(rows[-1][1], 100)  # 10+20+30+40
        conn.close()

    # --- Regression: pragma nesting bug (INTEGRITY_CHECK case) ---

    def test10_integrity_check_does_not_crash(self):
        """PRAGMA integrity_check must not crash — regression for pragma dispatch nesting bug"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, val text)")
        conn.commit()
        for i in range(10):
            c.execute("insert into t values (?, ?)", (i, f"val{i}"))
        conn.commit()
        c.execute("pragma integrity_check")
        result = c.fetchone()[0]
        self.assertEqual(result, "ok")
        conn.close()

    # --- Regression: litetreeCheckExecCommand placement ---

    def test11_branch_log_records_executed_sql(self):
        """branch_log must contain the SQL commands that ran (regression: wrong placement in sqlite3VtabImportErrmsg)"""
        conn = connect()
        c = conn.cursor()
        c.execute("pragma branch_log")
        if c.fetchone()[0] == "disabled":
            self.skipTest("branch_log disabled in this build")
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values ('hello')")
        conn.commit()
        c.execute("pragma branch_log(master)")
        rows = c.fetchall()
        all_sql = " ".join(str(col) for row in rows for col in row).lower()
        self.assertIn("create table", all_sql)
        self.assertIn("insert", all_sql)
        conn.close()

    # --- Regression: rename_branch pragma alphabetical ordering ---

    def test12_rename_branch_pragma_found(self):
        """PRAGMA rename_branch must work (regression: entry out of alphabetical order in pragma table)"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("pragma new_branch=old_name at master.1")
        c.execute("pragma rename_branch old_name new_name")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("new_name", names)
        self.assertNotIn("old_name", names)
        conn.close()

    # --- Regression: zTail not advancing past branch pragmas ---

    def test13_branch_pragma_after_dml_works(self):
        """Branch pragmas must work normally after DML statements (regression: zTail advancement)"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values ('row1')")
        conn.commit()
        c.execute("pragma new_branch=after_dml at master.2")
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "after_dml")
        c.execute("insert into t values ('branch_row')")
        conn.commit()
        c.execute("pragma branch=master")
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()


if __name__ == '__main__':
    unittest.main()
