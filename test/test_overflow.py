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

DB = "overflow_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')


class TestOverflow(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    def test01_overflow_visible_in_branch_created_after(self):
        """Large value committed on master is visible in a branch created from that commit"""
        conn = connect()
        c = conn.cursor()
        big = "A" * 50000
        c.execute("create table t(data text)")
        conn.commit()
        c.execute("insert into t values (?)", (big,))
        conn.commit()  # commit 2
        c.execute("pragma new_branch=b1 at master.2")
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test02_overflow_on_child_not_visible_on_parent(self):
        """Large value inserted on child branch is not visible on parent"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(data text)")
        conn.commit()
        c.execute("pragma new_branch=child at master.1")
        big = "B" * 50000
        c.execute("insert into t values (?)", (big,))
        conn.commit()
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test03_overflow_at_historical_commit(self):
        """Reading at a commit before an overflow insert returns no overflow data"""
        conn = connect()
        c = conn.cursor()
        big = "C" * 50000
        c.execute("create table t(id integer, data text)")
        conn.commit()
        c.execute("insert into t values (1, 'small')")
        conn.commit()  # commit 2
        c.execute("insert into t values (2, ?)", (big,))
        conn.commit()  # commit 3
        c.execute("pragma branch=master.2")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        c.execute("select data from t")
        self.assertEqual(c.fetchone()[0], "small")
        conn.close()

    def test04_overflow_update_old_commit_unchanged(self):
        """After updating a large value, the old commit still returns the original"""
        conn = connect()
        c = conn.cursor()
        big1 = "D" * 50000
        big2 = "E" * 60000
        c.execute("create table t(id integer, data text)")
        conn.commit()
        c.execute("insert into t values (1, ?)", (big1,))
        conn.commit()  # commit 2
        c.execute("update t set data=? where id=1", (big2,))
        conn.commit()  # commit 3
        c.execute("pragma branch=master.2")
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test05_overflow_delete_branch_isolation(self):
        """Deleting an overflow row on one branch leaves it intact on the other"""
        conn = connect()
        c = conn.cursor()
        big = "F" * 50000
        c.execute("create table t(id integer, data text)")
        conn.commit()
        c.execute("insert into t values (1, ?)", (big,))
        conn.commit()  # commit 2
        c.execute("pragma new_branch=del_branch at master.2")
        c.execute("delete from t where id=1")
        conn.commit()
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        c.execute("pragma branch=master")
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test06_overflow_multiple_columns_same_row(self):
        """A row with multiple large columns is fully readable"""
        conn = connect()
        c = conn.cursor()
        big1 = "G" * 5000
        big2 = "H" * 8000
        big3 = "I" * 12000
        c.execute("create table t(a text, b text, c text)")
        conn.commit()
        c.execute("insert into t values (?, ?, ?)", (big1, big2, big3))
        conn.commit()
        c.execute("select length(a), length(b), length(c) from t")
        row = c.fetchone()
        self.assertEqual(row[0], 5000)
        self.assertEqual(row[1], 8000)
        self.assertEqual(row[2], 12000)
        conn.close()

    def test07_overflow_concurrent_readers_different_branches(self):
        """Two connections each reading overflow from different branches don't interfere"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        big = "J" * 50000
        c1.execute("create table t(data text)")
        conn1.commit()
        c1.execute("insert into t values (?)", (big,))
        conn1.commit()  # master commit 2
        c1.execute("pragma new_branch=small_branch at master.1")
        c1.execute("insert into t values ('small')")
        conn1.commit()
        # conn1 is on small_branch, conn2 stays on master
        c2.execute("select length(data) from t")
        self.assertEqual(c2.fetchone()[0], 50000)
        c1.execute("select data from t")
        self.assertEqual(c1.fetchone()[0], "small")
        conn1.close()
        conn2.close()

    def test08_overflow_survives_truncate(self):
        """An overflow row at the kept commit is accessible after truncating later commits"""
        conn = connect()
        c = conn.cursor()
        big = "K" * 50000
        c.execute("create table t(data text)")
        conn.commit()
        c.execute("insert into t values (?)", (big,))
        conn.commit()  # commit 2
        c.execute("insert into t values ('extra1')")
        conn.commit()  # commit 3
        c.execute("insert into t values ('extra2')")
        conn.commit()  # commit 4
        c.execute("pragma branch_truncate(master.2)")
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 50000)
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()

    def test09_overflow_survives_forward_merge(self):
        """Overflow rows in a child branch are intact after forward-merging to parent"""
        conn = connect()
        c = conn.cursor()
        big = "L" * 50000
        c.execute("create table t(id integer, data text)")
        conn.commit()  # master commit 1
        c.execute("pragma new_branch=child")
        c.execute("insert into t values (1, ?)", (big,))
        conn.commit()  # child commit 2
        c.execute("insert into t values (2, 'small')")
        conn.commit()  # child commit 3
        c.execute("pragma branch_merge --forward master child 2")
        c.execute("pragma branch=master")
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test10_overflow_with_returning_clause(self):
        """INSERT ... RETURNING works when the inserted row requires overflow pages"""
        conn = connect()
        c = conn.cursor()
        big = "M" * 50000
        c.execute("create table t(id integer primary key, data text)")
        conn.commit()
        c.execute("insert into t(data) values (?) returning id", (big,))
        row = c.fetchone()
        self.assertIsNotNone(row)
        inserted_id = row[0]
        conn.commit()
        c.execute("select length(data) from t where id=?", (inserted_id,))
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test11_overflow_very_large_value(self):
        """A 1 MB value survives insert and retrieval"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(data text)")
        conn.commit()
        big = "N" * 1000000
        c.execute("insert into t values (?)", (big,))
        conn.commit()
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 1000000)
        conn.close()

    def test12_overflow_boundary_sizes(self):
        """Values at and around the overflow boundary (~4045 bytes) all round-trip correctly"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer, data text)")
        conn.commit()
        for size in [4000, 4044, 4045, 4046, 4096, 4097, 8192, 16384]:
            c.execute("insert into t values (?, ?)", (size, "x" * size))
            conn.commit()
        c.execute("select id, length(data) from t order by id")
        for row in c.fetchall():
            self.assertEqual(row[0], row[1], f"size {row[0]} did not round-trip correctly")
        conn.close()

    def test13_overflow_three_branches_same_table(self):
        """Three branches each hold different large values in the same table"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(branch text, data text)")
        conn.commit()  # commit 1
        c.execute("pragma new_branch=b1 at master.1")
        c.execute("insert into t values ('b1', ?)", ("P" * 50000,))
        conn.commit()
        c.execute("pragma new_branch=b2 at master.1")
        c.execute("insert into t values ('b2', ?)", ("Q" * 60000,))
        conn.commit()
        c.execute("pragma new_branch=b3 at master.1")
        c.execute("insert into t values ('b3', ?)", ("R" * 70000,))
        conn.commit()
        c.execute("pragma branch=b1")
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 50000)
        c.execute("pragma branch=b2")
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 60000)
        c.execute("pragma branch=b3")
        c.execute("select length(data) from t")
        self.assertEqual(c.fetchone()[0], 70000)
        conn.close()

    def test14_overflow_update_in_branch_preserves_parent(self):
        """Updating a large value on a branch does not affect the parent branch"""
        conn = connect()
        c = conn.cursor()
        big_orig = "S" * 50000
        big_new  = "T" * 80000
        c.execute("create table t(id integer, data text)")
        conn.commit()
        c.execute("insert into t values (1, ?)", (big_orig,))
        conn.commit()  # commit 2
        c.execute("pragma new_branch=update_branch at master.2")
        c.execute("update t set data=? where id=1", (big_new,))
        conn.commit()
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 80000)
        c.execute("pragma branch=master")
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()

    def test15_overflow_readable_after_many_subsequent_commits(self):
        """An overflow row remains readable after many small commits on the same branch"""
        conn = connect()
        c = conn.cursor()
        big = "U" * 50000
        c.execute("create table t(id integer, data text)")
        conn.commit()
        c.execute("insert into t values (1, ?)", (big,))
        conn.commit()  # commit 2
        c.execute("create table small(n integer)")
        conn.commit()
        for i in range(10):
            c.execute("insert into small values (?)", (i,))
            conn.commit()
        c.execute("select length(data) from t where id=1")
        self.assertEqual(c.fetchone()[0], 50000)
        conn.close()


if __name__ == '__main__':
    unittest.main()
