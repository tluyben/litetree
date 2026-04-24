#
# Copyright defined in LICENSE.txt
#
import unittest
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

DB = "isolation_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')


class TestBranchIsolation(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    def test01_insert_not_visible_on_other_branch(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(name text)")
        conn.commit()
        c.execute("insert into t values ('master_row')")
        conn.commit()  # commit 2
        c.execute("pragma new_branch=b at master.1")
        c.execute("insert into t values ('branch_row')")
        conn.commit()
        c.execute("select name from t")
        self.assertEqual(c.fetchone()[0], "branch_row")
        c.execute("pragma branch=master")
        c.execute("select name from t")
        self.assertEqual(c.fetchone()[0], "master_row")
        conn.close()

    def test02_update_isolated_between_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, val text)")
        conn.commit()
        c.execute("insert into t values (1, 'original')")
        conn.commit()  # commit 2
        c.execute("pragma new_branch=b at master.2")
        c.execute("update t set val='changed' where id=1")
        conn.commit()
        c.execute("select val from t where id=1")
        self.assertEqual(c.fetchone()[0], "changed")
        c.execute("pragma branch=master")
        c.execute("select val from t where id=1")
        self.assertEqual(c.fetchone()[0], "original")
        conn.close()

    def test03_delete_isolated_between_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer)")
        conn.commit()
        c.execute("insert into t values (1)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("delete from t where id=1")
        conn.commit()
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()

    def test04_add_column_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("alter table t add column extra text")
        conn.commit()
        c.execute("pragma table_info(t)")
        cols = [row[1] for row in c.fetchall()]
        self.assertIn("extra", cols)
        c.execute("pragma branch=master")
        c.execute("pragma table_info(t)")
        cols = [row[1] for row in c.fetchall()]
        self.assertNotIn("extra", cols)
        conn.close()

    def test05_drop_table_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("drop table t")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("select * from t")
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test06_create_index_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, val text)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("create index idx_val on t(val)")
        conn.commit()
        c.execute("pragma index_list(t)")
        self.assertEqual(len(c.fetchall()), 1)
        c.execute("pragma branch=master")
        c.execute("pragma index_list(t)")
        self.assertEqual(len(c.fetchall()), 0)
        conn.close()

    def test07_two_branches_from_same_commit_are_independent(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()  # commit 1
        c.execute("pragma new_branch=b1 at master.1")
        c.execute("insert into t values ('from_b1')")
        conn.commit()
        c.execute("pragma new_branch=b2 at master.1")
        c.execute("insert into t values ('from_b2')")
        conn.commit()
        c.execute("select val from t")
        self.assertEqual(c.fetchone()[0], "from_b2")
        c.execute("pragma branch=b1")
        c.execute("select val from t")
        self.assertEqual(c.fetchone()[0], "from_b1")
        conn.close()

    def test08_historical_commit_is_readonly(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values ('v1')")
        conn.commit()  # commit 2
        c.execute("pragma branch=master.1")
        c.execute("insert into t values ('should_fail')")
        with self.assertRaises(sqlite3.OperationalError):
            conn.commit()  # error raised at commit, not execute
        conn.close()

    def test09_historical_commit_shows_correct_snapshot(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values ('v1')")
        conn.commit()  # commit 2
        c.execute("insert into t values ('v2')")
        conn.commit()  # commit 3
        c.execute("pragma branch=master.2")
        c.execute("select val from t")
        self.assertListEqual(c.fetchall(), [("v1",)])
        c.execute("pragma branch=master.1")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test10_null_values_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, val text)")
        conn.commit()
        c.execute("insert into t values (1, NULL)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set val='not_null' where id=1")
        conn.commit()
        c.execute("select val from t where id=1")
        self.assertEqual(c.fetchone()[0], "not_null")
        c.execute("pragma branch=master")
        c.execute("select val from t where id=1")
        self.assertIsNone(c.fetchone()[0])
        conn.close()

    def test11_empty_branch_shows_no_data(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values ('row1')")
        conn.commit()  # commit 2
        c.execute("pragma new_branch=empty_b at master.1")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test12_autoincrement_independent_across_branches(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer primary key autoincrement, val text)")
        conn.commit()
        c.execute("insert into t(val) values ('a')")
        conn.commit()  # id=1 on both
        c.execute("pragma new_branch=b at master.2")
        c.execute("insert into t(val) values ('b')")
        conn.commit()  # id=2 on branch
        c.execute("pragma branch=master")
        c.execute("insert into t(val) values ('c')")
        conn.commit()  # id=2 on master (independent counter)
        c.execute("select id, val from t order by id")
        self.assertListEqual(c.fetchall(), [(1, "a"), (2, "c")])
        c.execute("pragma branch=b")
        c.execute("select id, val from t order by id")
        self.assertListEqual(c.fetchall(), [(1, "a"), (2, "b")])
        conn.close()

    def test13_large_row_count_isolated(self):
        """1000 rows on one branch must not appear on the other"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(n integer)")
        conn.commit()
        c.execute("pragma new_branch=big_branch")
        for i in range(100):
            c.executemany("insert into t values (?)", [(j,) for j in range(i * 10, (i + 1) * 10)])
            conn.commit()
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1000)
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test14_integer_boundary_values_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(tiny integer, big integer)")
        conn.commit()
        c.execute("insert into t values (127, 9223372036854775807)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set tiny=-128, big=-9223372036854775808")
        conn.commit()
        c.execute("select * from t")
        self.assertEqual(c.fetchone(), (-128, -9223372036854775808))
        c.execute("pragma branch=master")
        c.execute("select * from t")
        self.assertEqual(c.fetchone(), (127, 9223372036854775807))
        conn.close()

    def test15_float_values_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val real)")
        conn.commit()
        c.execute("insert into t values (3.14159)")
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set val=2.71828")
        conn.commit()
        c.execute("select val from t")
        self.assertAlmostEqual(c.fetchone()[0], 2.71828, places=4)
        c.execute("pragma branch=master")
        c.execute("select val from t")
        self.assertAlmostEqual(c.fetchone()[0], 3.14159, places=4)
        conn.close()

    def test16_unicode_text_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("insert into t values (?)", ("Hello, 世界 🌍",))
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set val=?", ("Привет мир",))
        conn.commit()
        c.execute("select val from t")
        self.assertEqual(c.fetchone()[0], "Привет мир")
        c.execute("pragma branch=master")
        c.execute("select val from t")
        self.assertEqual(c.fetchone()[0], "Hello, 世界 🌍")
        conn.close()

    def test17_blob_values_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val blob)")
        conn.commit()
        data1 = bytes(range(256))
        data2 = bytes(reversed(range(256)))
        c.execute("insert into t values (?)", (data1,))
        conn.commit()
        c.execute("pragma new_branch=b")
        c.execute("update t set val=?", (data2,))
        conn.commit()
        c.execute("select val from t")
        self.assertEqual(bytes(c.fetchone()[0]), data2)
        c.execute("pragma branch=master")
        c.execute("select val from t")
        self.assertEqual(bytes(c.fetchone()[0]), data1)
        conn.close()

    def test18_multiple_tables_isolated(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table a(val text)")
        c.execute("create table b(val text)")
        conn.commit()
        c.execute("insert into a values ('a_master')")
        c.execute("insert into b values ('b_master')")
        conn.commit()
        c.execute("pragma new_branch=br")
        c.execute("update a set val='a_branch'")
        conn.commit()
        c.execute("select val from a")
        self.assertEqual(c.fetchone()[0], "a_branch")
        c.execute("select val from b")
        self.assertEqual(c.fetchone()[0], "b_master")
        c.execute("pragma branch=master")
        c.execute("select val from a")
        self.assertEqual(c.fetchone()[0], "a_master")
        c.execute("select val from b")
        self.assertEqual(c.fetchone()[0], "b_master")
        conn.close()

    def test19_concurrent_writes_to_different_branches(self):
        """Two connections write to separate branches without interfering"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(val text)")
        conn1.commit()
        c1.execute("pragma new_branch=b1")
        c2.execute("pragma new_branch=b2 at master.1")
        c1.execute("insert into t values ('from_b1')")
        conn1.commit()
        c2.execute("insert into t values ('from_b2')")
        conn2.commit()
        c1.execute("select val from t")
        self.assertEqual(c1.fetchone()[0], "from_b1")
        c2.execute("select val from t")
        self.assertEqual(c2.fetchone()[0], "from_b2")
        conn1.close()
        conn2.close()

    def test20_branch_data_persists_across_reopen(self):
        """Data written to a branch survives closing and reopening the database"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("pragma new_branch=persist_branch")
        c.execute("insert into t values ('persisted')")
        conn.commit()
        conn.close()
        conn = connect()
        c = conn.cursor()
        c.execute("pragma branch=persist_branch")
        c.execute("select val from t")
        self.assertEqual(c.fetchone()[0], "persisted")
        conn.close()


if __name__ == '__main__':
    unittest.main()
