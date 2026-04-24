#
# Copyright defined in LICENSE.txt
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

DB = "merge_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')

def branch_info(c, name):
    c.execute(f"pragma branch_info({name})")
    return json.loads(c.fetchone()[0])


class TestMerge(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    def _setup_parent_child(self, num_child_commits=3):
        """master: create table + 1 row (2 commits). child: num_child_commits more rows."""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()  # master commit 1
        c.execute("insert into t values ('m1')")
        conn.commit()  # master commit 2
        c.execute("pragma new_branch=child at master.2")
        for i in range(num_child_commits):
            c.execute("insert into t values (?)", (f"c{i + 1}",))
            conn.commit()
        return conn, c

    def test01_merge_one_commit_at_a_time(self):
        conn, c = self._setup_parent_child(3)
        for expected in [3, 4, 5]:
            c.execute("pragma branch_merge --forward master child 1")
            self.assertListEqual(c.fetchall(), [("OK",)])
            self.assertEqual(branch_info(c, "master")["total_commits"], expected)
        conn.close()

    def test02_merge_all_at_once_same_result_as_incremental(self):
        conn, c = self._setup_parent_child(3)
        c.execute("pragma branch_merge --forward master child 3")
        c.execute("pragma branch=master")
        c.execute("select val from t order by rowid")
        rows_bulk = c.fetchall()
        conn.close()

        delete_db()
        conn, c = self._setup_parent_child(3)
        for _ in range(3):
            c.execute("pragma branch_merge --forward master child 1")
        c.execute("pragma branch=master")
        c.execute("select val from t order by rowid")
        rows_incremental = c.fetchall()
        conn.close()

        self.assertEqual(rows_bulk, rows_incremental)

    def test03_merge_after_child_truncate(self):
        """Truncate child to remove last 2 commits, then merge remaining 2"""
        conn, c = self._setup_parent_child(4)  # child total_commits = 6
        total = branch_info(c, "child")["total_commits"]
        keep_at = total - 2  # 4 — removes last 2 child commits
        c.execute(f"pragma branch_truncate(child.{keep_at})")
        self.assertEqual(branch_info(c, "child")["total_commits"], 4)
        c.execute("pragma branch_merge --forward master child 2")
        self.assertListEqual(c.fetchall(), [("OK",)])
        self.assertEqual(branch_info(c, "master")["total_commits"], 4)
        conn.close()

    def test04_merge_zero_commits_errors(self):
        conn, c = self._setup_parent_child(3)
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child 0")
        conn.close()

    def test05_merge_negative_count_errors(self):
        conn, c = self._setup_parent_child(3)
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child -1")
        conn.close()

    def test06_merge_out_of_range_count_errors(self):
        conn, c = self._setup_parent_child(3)
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child 100")
        conn.close()

    def test07_merge_with_overflow_pages(self):
        """Child has large-value rows; they are intact after forward merge to parent"""
        conn = connect()
        c = conn.cursor()
        big = "X" * 50000
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

    def test08_merge_commit_count_increases_correctly(self):
        conn, c = self._setup_parent_child(4)
        before = branch_info(c, "master")["total_commits"]
        c.execute("pragma branch_merge --forward master child 3")
        after = branch_info(c, "master")["total_commits"]
        self.assertEqual(after, before + 3)
        conn.close()

    def test09_pre_merge_commits_still_readable(self):
        """Commits on master before the merge remain accessible by number"""
        conn, c = self._setup_parent_child(3)
        c.execute("pragma branch=master.1")
        c.execute("select count(*) from t")
        count_at_1 = c.fetchone()[0]  # 0 (empty at commit 1)
        c.execute("pragma branch=master")
        c.execute("pragma branch_merge --forward master child 3")
        c.execute("pragma branch=master.1")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], count_at_1)
        conn.close()

    def test10_child_can_still_write_after_partial_merge(self):
        conn, c = self._setup_parent_child(3)
        c.execute("pragma branch_merge --forward master child 2")
        c.execute("pragma branch=child")
        c.execute("insert into t values ('post_merge')")
        conn.commit()
        c.execute("select val from t")
        rows = [r[0] for r in c.fetchall()]
        self.assertIn("post_merge", rows)
        conn.close()

    def test11_merge_grandchild_to_child(self):
        """Grandchild branch can be merged up to child"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(val text)")
        conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('child1')")
        conn.commit()
        c.execute("pragma new_branch=grandchild")
        c.execute("insert into t values ('grand1')")
        conn.commit()
        c.execute("pragma branch=child")
        before = branch_info(c, "child")["total_commits"]
        c.execute("pragma branch_merge --forward child grandchild 1")
        after = branch_info(c, "child")["total_commits"]
        self.assertEqual(after, before + 1)
        conn.close()

    def test12_concurrent_reader_during_merge(self):
        """A second connection can read the parent branch while a merge is in progress"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(val text)")
        conn1.commit()
        c1.execute("pragma new_branch=child")
        for i in range(5):
            c1.execute("insert into t values (?)", (f"row{i}",))
            conn1.commit()
        c2.execute("pragma branch=master")
        c2.execute("select count(*) from t")
        count_before = c2.fetchone()[0]
        c1.execute("pragma branch_merge --forward master child 5")
        c2.execute("select count(*) from t")
        count_after = c2.fetchone()[0]
        self.assertGreaterEqual(count_after, count_before)
        conn1.close()
        conn2.close()

    def test13_merge_source_commit_updates(self):
        """After merge, child's source_commit advances to the new parent head"""
        conn, c = self._setup_parent_child(3)
        info_before = branch_info(c, "child")
        c.execute("pragma branch_merge --forward master child 3")
        info_after = branch_info(c, "child")
        self.assertEqual(info_after["source_branch"], "master")
        self.assertGreater(info_after["source_commit"], info_before["source_commit"])
        conn.close()

    def test14_full_merge_data_correct(self):
        """All child rows appear on master after merging all commits"""
        conn, c = self._setup_parent_child(5)
        c.execute("pragma branch_merge --forward master child 5")
        c.execute("pragma branch=master")
        c.execute("select val from t order by rowid")
        rows = [r[0] for r in c.fetchall()]
        self.assertIn("m1", rows)
        for i in range(1, 6):
            self.assertIn(f"c{i}", rows)
        conn.close()

    def test15_merge_schema_add_column_propagates(self):
        """Child adds a column; after merge, master has the new column"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(id integer)")
        conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("alter table t add column extra text default 'def'")
        conn.commit()
        c.execute("insert into t values (1, 'val')")
        conn.commit()
        c.execute("pragma branch_merge --forward master child 2")
        c.execute("pragma branch=master")
        c.execute("pragma table_info(t)")
        cols = [row[1] for row in c.fetchall()]
        self.assertIn("extra", cols)
        conn.close()


if __name__ == '__main__':
    unittest.main()
