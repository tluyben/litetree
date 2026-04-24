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

DB = "errors_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')


class TestErrors(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    def test01_switch_to_nonexistent_branch_errors(self):
        conn = connect()
        c = conn.cursor()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch=nonexistent_branch")
        conn.close()

    def test02_new_branch_duplicate_name_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=dup at master.1")
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=dup at master.1")
        conn.close()

    def test03_new_branch_beyond_head_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=b at master.999")
        conn.close()

    def test04_write_to_historical_commit_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("insert into t values ('v1')")
        conn.commit()  # commit 2
        c.execute("pragma branch=master.1")
        c.execute("insert into t values ('should_fail')")
        with self.assertRaises(sqlite3.OperationalError):
            conn.commit()  # access permission denied — error raised at commit, not execute
        conn.close()

    def test05_del_current_branch_errors(self):
        """Deleting the branch currently in use by this connection must fail"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma branch")
        current = c.fetchone()[0]
        with self.assertRaises(sqlite3.OperationalError):
            c.execute(f"pragma del_branch({current})")
        conn.close()

    def test06_del_nonexistent_branch_errors(self):
        conn = connect()
        c = conn.cursor()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma del_branch(ghost_branch)")
        conn.close()

    def test07_rename_to_existing_name_errors(self):
        """rename_branch to a name that already exists must raise an error, not silently create duplicate branches"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=b1")
        c.execute("pragma new_branch=b2")
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma rename_branch b1 b2")
        # Branch list must remain clean — no duplicates
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertEqual(len(names), len(set(names)), "duplicate branch names after failed rename")
        self.assertIn("b1", names)
        self.assertIn("b2", names)
        conn.close()

    def test07b_rename_to_name_with_equals_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=valid_name")
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma rename_branch valid_name bad=name")
        conn.close()

    def test08_merge_zero_commits_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('x')")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child 0")
        conn.close()

    def test09_merge_negative_count_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('x')")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child -1")
        conn.close()

    def test10_merge_nonexistent_branch_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master ghost_branch 1")
        conn.close()

    def test11_merge_count_exceeds_available_commits_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('x')")
        conn.commit()  # only 1 child-specific commit
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_merge --forward master child 5")
        conn.close()

    def test12_branch_info_nonexistent_returns_none(self):
        """branch_info with an unknown name returns None (no error raised)"""
        conn = connect()
        c = conn.cursor()
        c.execute("pragma branch_info(ghost_branch)")
        self.assertIsNone(c.fetchone())
        conn.close()

    def test13_truncate_beyond_head_errors(self):
        """branch_truncate to a commit beyond the branch head must raise an error, not silently do nothing"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        c.execute("insert into t values ('v1')")
        conn.commit()  # commit 2
        c.execute("insert into t values ('v2')")
        conn.commit()  # commit 3 = head
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_truncate(master.999)")
        # Data must be untouched
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 2)
        conn.close()

    def test14_rename_nonexistent_branch_errors(self):
        conn = connect()
        c = conn.cursor()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma rename_branch ghost_branch new_name")
        conn.close()

    def test15_error_does_not_leave_connection_broken(self):
        """After an OperationalError the connection can still run normal queries"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)")
        conn.commit()
        try:
            c.execute("pragma branch=nonexistent_branch")
        except sqlite3.OperationalError:
            pass
        # Connection must still be usable
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "master")
        c.execute("insert into t values ('ok')")
        conn.commit()
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()


if __name__ == '__main__':
    unittest.main()
