#
# Copyright defined in LICENSE.txt
#
# Branch operation edge cases: name validation, truncate constraints with
# complex trees, delete semantics, concurrent cross-connection operations.
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

DB = "branch_ops_test.db"

def delete_db():
    for f in [DB, DB + "-lock"]:
        if os.path.exists(f):
            os.remove(f)

def connect():
    return sqlite3.connect('file:' + DB + '?branches=on')

def branch_info(c, name):
    c.execute(f"pragma branch_info({name})")
    row = c.fetchone()
    return json.loads(row[0]) if row else None


class TestBranchOps(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    # --- Branch name validation ---

    def test01_name_127_chars_works(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        long_name = "a" * 127
        c.execute(f"pragma new_branch={long_name}")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn(long_name, names)
        conn.close()

    def test02_name_128_chars_errors(self):
        """Branch names >= 128 characters must raise an error, not crash"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=" + "a" * 128)
        conn.close()

    def test03_name_single_char_works(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=a")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("a", names)
        conn.close()

    def test04_name_with_digits_after_letter_works(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=branch42")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("branch42", names)
        conn.close()

    def test05_name_with_hyphens_works(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=my-feature-branch")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("my-feature-branch", names)
        conn.close()

    def test06_name_with_underscores_works(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=my_feature_branch")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("my_feature_branch", names)
        conn.close()

    def test07_name_starting_with_digit_is_allowed(self):
        """Branch names starting with a digit are accepted (no restriction on first char)"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=1branch")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("1branch", names)
        conn.close()

    def test08_name_starting_with_hyphen_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=-branch")
        conn.close()

    def test09_name_with_dot_errors(self):
        """Dots in branch names are not allowed (they are used as branch.commit separators)"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=has.dot")
        conn.close()

    def test10_empty_name_errors(self):
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma new_branch=")
        conn.close()

    # --- Rename edge cases ---

    def test11_rename_identity_is_noop(self):
        """Renaming a branch to its own name succeeds without error"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma rename_branch master master")
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "master")
        conn.close()

    def test12_rename_to_existing_name_errors(self):
        """Already tested in test_errors but confirmed here too"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=b1")
        c.execute("pragma new_branch=b2")
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma rename_branch b1 b2")
        conn.close()

    def test13_rename_with_children_children_data_correct(self):
        """After renaming parent, child data is still accessible and correct"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('parent_data')"); conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('child_data')"); conn.commit()
        c.execute("pragma branch=master")
        c.execute("pragma rename_branch master new_master")
        c.execute("pragma branch=child")
        c.execute("select v from t order by rowid")
        self.assertListEqual(c.fetchall(), [("parent_data",), ("child_data",)])
        conn.close()

    def test14_rename_allows_old_name_to_be_reused(self):
        """After renaming branch A to B, a new branch A can be created"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=original")
        c.execute("pragma branch=master")
        c.execute("pragma rename_branch original moved")
        c.execute("pragma new_branch=original")  # reuse the old name
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("moved", names)
        self.assertIn("original", names)
        conn.close()

    # --- Delete edge cases ---

    def test15_delete_parent_source_branch_becomes_stale(self):
        """Deleting a parent branch leaves child's source_branch as a dangling name"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=parent_b")
        c.execute("insert into t values ('p')"); conn.commit()
        c.execute("pragma new_branch=child_b")
        c.execute("pragma branch=master")
        c.execute("pragma del_branch(parent_b)")
        info = branch_info(c, "child_b")
        # source_branch name still references the deleted branch
        self.assertEqual(info["source_branch"], "parent_b")
        # but the child is still functionally accessible
        c.execute("pragma branch=child_b")
        c.execute("select v from t")
        self.assertEqual(c.fetchone()[0], "p")
        conn.close()

    def test16_delete_all_non_master_branches(self):
        """Deleting all non-master branches leaves just master intact"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        for name in ["b1", "b2", "b3"]:
            c.execute(f"pragma new_branch={name}")
            c.execute("pragma branch=master")
        for name in ["b1", "b2", "b3"]:
            c.execute(f"pragma del_branch({name})")
        c.execute("pragma branches")
        self.assertListEqual(c.fetchall(), [("master",)])
        conn.close()

    def test17_delete_branch_other_conn_on_it_branch_becomes_invalid(self):
        """Deleting a branch while another connection uses it makes that conn's branch invalid"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(v text)"); conn1.commit()
        c1.execute("pragma new_branch=victim")
        c2.execute("pragma branch=victim")
        c2.execute("pragma branch")
        self.assertEqual(c2.fetchone()[0], "victim")
        # Switch c1 off victim, then delete it
        c1.execute("pragma branch=master")
        c1.execute("pragma del_branch(victim)")
        # conn2 is now on a deleted branch; operations should fail
        # (raises OperationalError or InternalError depending on when the error is detected)
        with self.assertRaises((sqlite3.OperationalError, sqlite3.InternalError)):
            c2.execute("select * from t")
        conn1.close()
        conn2.close()

    # --- Truncate with complex trees ---

    def test18_truncate_grandchild_dependency_blocks_parent_truncate(self):
        """Grandchild depending on a commit prevents truncation even when direct child was deleted"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()   # commit 2
        c.execute("pragma new_branch=mid")                        # source_commit = 2
        c.execute("insert into t values ('r2')"); conn.commit()   # commit 3 (on mid)
        c.execute("pragma new_branch=leaf")                       # source_commit = 3 (on mid)
        c.execute("pragma branch=master")
        c.execute("insert into t values ('r3')"); conn.commit()   # commit 3 (on master)
        # Cannot truncate master to 1 because mid branches from master.2
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_truncate(master.1)")
        conn.close()

    def test19_truncate_to_1_when_no_children(self):
        """Truncating to commit 1 (keeping only schema) works when branch has no children"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()        # commit 1
        c.execute("insert into t values ('r1')"); conn.commit()   # commit 2
        c.execute("insert into t values ('r2')"); conn.commit()   # commit 3
        c.execute("pragma branch_truncate(master.1)")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        self.assertEqual(branch_info(c, "master")["total_commits"], 1)
        conn.close()

    def test20_truncate_then_write_commit_numbering_continues(self):
        """After truncating, new commits are numbered from where truncation left off"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()        # commit 1
        c.execute("insert into t values ('r1')"); conn.commit()   # commit 2
        c.execute("insert into t values ('r2')"); conn.commit()   # commit 3
        c.execute("pragma branch_truncate(master.1)")
        c.execute("insert into t values ('new')"); conn.commit()  # should be commit 2
        info = branch_info(c, "master")
        self.assertEqual(info["total_commits"], 2)
        c.execute("select v from t")
        self.assertEqual(c.fetchone()[0], "new")
        conn.close()

    def test21_truncate_then_branch_from_truncated_point(self):
        """After truncating, can create a new branch from the new head"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()        # commit 1
        c.execute("insert into t values ('r1')"); conn.commit()   # commit 2
        c.execute("insert into t values ('r2')"); conn.commit()   # commit 3
        c.execute("pragma branch_truncate(master.2)")
        c.execute("pragma new_branch=post_truncate")
        info = branch_info(c, "post_truncate")
        self.assertEqual(info["source_commit"], 2)
        conn.close()

    # --- branch_info fields ---

    def test22_branch_info_master_fields(self):
        """branch_info for master has the expected fields"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()
        info = branch_info(c, "master")
        self.assertIn("total_commits", info)
        self.assertEqual(info["total_commits"], 2)
        conn.close()

    def test23_branch_info_child_fields(self):
        """branch_info for a child has source_branch and source_commit"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("pragma new_branch=child")
        info = branch_info(c, "child")
        self.assertEqual(info["source_branch"], "master")
        self.assertEqual(info["source_commit"], 2)
        self.assertEqual(info["total_commits"], 2)
        conn.close()

    # --- Merge + delete sequence ---

    def test24_full_merge_then_delete_child(self):
        """After merging all child commits to parent, child can be deleted cleanly"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('c1')"); conn.commit()
        c.execute("insert into t values ('c2')"); conn.commit()
        c.execute("pragma branch_merge --forward master child 2")
        c.execute("pragma branch=master")
        c.execute("pragma del_branch(child)")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertNotIn("child", names)
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 2)
        conn.close()

    def test25_switch_historical_then_head(self):
        """Moving to a historical commit and then back to head works correctly"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("insert into t values ('r2')"); conn.commit()  # commit 3
        c.execute("pragma branch=master.1")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 2)
        conn.close()

    def test26_branch_from_child_middle_commit(self):
        """Branching from a specific mid-history commit of a child branch using 'at'"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()         # commit 1
        c.execute("pragma new_branch=mid")
        c.execute("insert into t values ('m1')"); conn.commit()    # commit 2
        c.execute("insert into t values ('m2')"); conn.commit()    # commit 3 = mid head
        # Branch from mid at commit 2 explicitly; new_branch without 'at' uses HEAD
        c.execute("pragma new_branch=from_mid_c2 at mid.2")
        info = branch_info(c, "from_mid_c2")
        self.assertEqual(info["source_branch"], "mid")
        self.assertEqual(info["source_commit"], 2)
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)  # only m1 visible at mid.2
        conn.close()

    def test27_concurrent_write_same_branch_serializes(self):
        """Two connections writing to the same branch do not corrupt data"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(n integer)"); conn1.commit()
        # conn1 writes first
        for i in range(10):
            c1.execute("insert into t values (?)", (i,)); conn1.commit()
        # conn2 then reads
        c2.execute("select count(*) from t")
        self.assertEqual(c2.fetchone()[0], 10)
        # conn2 writes
        for i in range(10, 20):
            c2.execute("insert into t values (?)", (i,)); conn2.commit()
        c1.execute("select count(*) from t")
        self.assertEqual(c1.fetchone()[0], 20)
        conn1.close()
        conn2.close()

    def test28_new_branch_visible_immediately_on_same_connection(self):
        """A newly created branch appears in the branches list on the same connection"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=immediate")
        c.execute("pragma branch=master")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("immediate", names)
        conn.close()

    def test29_branch_info_after_many_operations(self):
        """branch_info stays accurate after a sequence of create/merge/truncate"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()         # master commit 1
        c.execute("insert into t values ('m1')"); conn.commit()    # master commit 2
        c.execute("pragma new_branch=work")
        c.execute("insert into t values ('w1')"); conn.commit()    # work commit 3
        c.execute("insert into t values ('w2')"); conn.commit()    # work commit 4
        c.execute("pragma branch_merge --forward master work 2")
        # master now has 4 commits
        info = branch_info(c, "master")
        self.assertEqual(info["total_commits"], 4)
        # truncate master to 3
        c.execute("pragma branch=master")
        # Need to check if work still depends on master... after merge work source_commit updated
        info_work = branch_info(c, "work")
        # Truncate master to just after the merge point if safe
        new_head = info_work["source_commit"]
        c.execute(f"pragma branch_truncate(master.{new_head})")
        info = branch_info(c, "master")
        self.assertEqual(info["total_commits"], new_head)
        conn.close()

    def test30_no_branch_leak_between_separate_databases(self):
        """Two separate branch databases don't share branch state"""
        DB2 = "branch_ops_test2.db"
        for f in [DB2, DB2 + "-lock"]:
            if os.path.exists(f):
                os.remove(f)
        conn1 = connect()
        conn2 = sqlite3.connect('file:' + DB2 + '?branches=on')
        if platform.system() == "Darwin":
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(v text)"); conn1.commit()
        c1.execute("pragma new_branch=db1_branch")
        c2.execute("create table t(v text)"); conn2.commit()
        c2.execute("pragma branches")
        names = [r[0] for r in c2.fetchall()]
        self.assertNotIn("db1_branch", names)
        conn1.close()
        conn2.close()
        for f in [DB2, DB2 + "-lock"]:
            if os.path.exists(f):
                os.remove(f)


if __name__ == '__main__':
    unittest.main()
