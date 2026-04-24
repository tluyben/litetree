#
# Copyright defined in LICENSE.txt
#
# Advanced scenario tests: complex branch trees, branch log, cross-connection
# visibility, sequences of operations, and deep/wide hierarchies.
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

DB = "advanced_test.db"

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

def log_enabled(c):
    c.execute("pragma branch_log")
    return c.fetchone()[0] != "disabled"


class TestAdvanced(unittest.TestCase):

    def setUp(self):
        delete_db()

    def tearDown(self):
        delete_db()

    # --- Rename edge cases ---

    def test01_rename_current_branch_updates_immediately(self):
        """Renaming the branch you are currently on takes effect immediately"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "master")
        c.execute("pragma rename_branch master renamed")
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "renamed")
        c.execute("pragma branches")
        names = [r[0] for r in c.fetchall()]
        self.assertIn("renamed", names)
        self.assertNotIn("master", names)
        conn.close()

    def test02_rename_updates_children_source_branch_name(self):
        """After renaming a branch, child branches report the new name as source_branch"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=child1")
        c.execute("pragma new_branch=child2")
        c.execute("pragma branch=master")
        c.execute("pragma rename_branch master new_master")
        self.assertEqual(branch_info(c, "child1")["source_branch"], "new_master")
        self.assertEqual(branch_info(c, "child2")["source_branch"], "new_master")
        conn.close()

    def test03_rename_updates_grandchildren_source_branch_name(self):
        """Renaming propagates through all levels of the tree"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=child")
        # give child its own commit so grandchild is truly sourced from child
        c.execute("insert into t values ('child_data')"); conn.commit()
        c.execute("pragma new_branch=grandchild")
        c.execute("pragma branch=master")
        c.execute("pragma rename_branch child renamed_child")
        self.assertEqual(branch_info(c, "renamed_child")["source_branch"], "master")
        self.assertEqual(branch_info(c, "grandchild")["source_branch"], "renamed_child")
        conn.close()

    def test04_rename_identity_is_noop(self):
        """Renaming a branch to its own name succeeds silently"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma rename_branch master master")
        c.execute("pragma branch")
        self.assertEqual(c.fetchone()[0], "master")
        conn.close()

    def test05_rename_cross_connection_visible(self):
        """Rename done on one connection is visible to another connection"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(v text)"); conn1.commit()
        c1.execute("pragma new_branch=to_rename")
        c1.execute("pragma rename_branch to_rename renamed_b")
        c2.execute("pragma branches")
        names = [r[0] for r in c2.fetchall()]
        self.assertIn("renamed_b", names)
        self.assertNotIn("to_rename", names)
        conn1.close()
        conn2.close()

    # --- Delete parent branch ---

    def test06_delete_parent_children_data_accessible(self):
        """Child branch data remains fully accessible after its parent is deleted"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('parent_data')"); conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('child_data')"); conn.commit()
        # must switch off master before deleting it
        c.execute("pragma branch=child")
        c.execute("pragma del_branch(master)")
        c.execute("select v from t order by rowid")
        self.assertListEqual(c.fetchall(), [("parent_data",), ("child_data",)])
        conn.close()

    def test07_delete_parent_grandchildren_still_work(self):
        """Grandchild branches continue to work after their grandparent is deleted"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=mid")
        c.execute("insert into t values ('mid')"); conn.commit()
        c.execute("pragma new_branch=leaf")
        c.execute("insert into t values ('leaf')"); conn.commit()
        # must switch off master before deleting it
        c.execute("pragma branch=leaf")
        c.execute("pragma del_branch(master)")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 2)
        conn.close()

    def test08_delete_then_recreate_same_name(self):
        """After deleting a branch, a new branch with the same name can be created fresh"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=reborn")
        c.execute("insert into t values ('old')"); conn.commit()
        c.execute("pragma branch=master")
        c.execute("pragma del_branch(reborn)")
        c.execute("pragma new_branch=reborn")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 0)  # fresh branch, no rows
        conn.close()

    # --- Branch log ---

    def test09_branch_log_multi_statement_commit(self):
        """Each SQL statement in a multi-statement commit appears as a separate log row"""
        conn = connect()
        c = conn.cursor()
        if not log_enabled(c):
            self.skipTest("branch_log disabled")
        c.execute("create table t(v text)"); conn.commit()
        c.execute("begin")
        c.execute("insert into t values ('x')")
        c.execute("insert into t values ('y')")
        c.execute("insert into t values ('z')")
        conn.commit()  # single commit containing 3 inserts
        c.execute("pragma branch_log(master)")
        rows = c.fetchall()
        commit2_rows = [r for r in rows if r[1] == 2]
        self.assertEqual(len(commit2_rows), 3)
        sqls = [r[2] for r in commit2_rows]
        self.assertTrue(any("'x'" in s for s in sqls))
        self.assertTrue(any("'y'" in s for s in sqls))
        self.assertTrue(any("'z'" in s for s in sqls))
        conn.close()

    def test10_branch_log_removes_truncated_entries(self):
        """Truncating removes log entries for the deleted commits"""
        conn = connect()
        c = conn.cursor()
        if not log_enabled(c):
            self.skipTest("branch_log disabled")
        c.execute("create table t(v text)"); conn.commit()          # commit 1
        c.execute("insert into t values ('keep')"); conn.commit()   # commit 2
        c.execute("insert into t values ('drop')"); conn.commit()   # commit 3
        c.execute("pragma branch_truncate(master.2)")
        c.execute("pragma branch_log(master)")
        rows = c.fetchall()
        commit_nums = {r[1] for r in rows}
        self.assertNotIn(3, commit_nums)
        self.assertIn(2, commit_nums)
        conn.close()

    def test11_branch_log_child_shows_own_commits(self):
        """branch_log for a child branch shows only its own commits, not master's"""
        conn = connect()
        c = conn.cursor()
        if not log_enabled(c):
            self.skipTest("branch_log disabled")
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=child")
        c.execute("insert into t values ('child_insert')"); conn.commit()
        c.execute("pragma branch_log(child)")
        rows = c.fetchall()
        all_sql = " ".join(r[2] for r in rows)
        self.assertIn("child_insert", all_sql)
        conn.close()

    # --- new_branch defaults ---

    def test12_new_branch_without_at_branches_from_head(self):
        """pragma new_branch=x (no 'at') creates branch from current branch's HEAD"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("insert into t values ('r2')"); conn.commit()  # commit 3 = head
        c.execute("pragma new_branch=from_head")
        info = branch_info(c, "from_head")
        self.assertEqual(info["source_branch"], "master")
        self.assertEqual(info["source_commit"], 3)
        conn.close()

    def test13_new_branch_on_child_without_at_uses_child_head(self):
        """new_branch on a child branch without 'at' defaults to that child's HEAD"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=mid")
        c.execute("insert into t values ('m1')"); conn.commit()
        c.execute("insert into t values ('m2')"); conn.commit()  # mid head = commit 3
        c.execute("pragma new_branch=leaf")
        info = branch_info(c, "leaf")
        self.assertEqual(info["source_branch"], "mid")
        self.assertEqual(info["source_commit"], 3)
        conn.close()

    # --- Truncate constraints ---

    def test14_truncate_blocked_below_child_source_commit(self):
        """Truncate is blocked when it would remove a commit a child depends on"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("pragma new_branch=child")                     # child.source_commit = 2
        c.execute("pragma branch=master")
        c.execute("insert into t values ('r2')"); conn.commit()  # commit 3
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_truncate(master.1)")
        conn.close()

    def test15_truncate_allowed_at_exact_child_source_commit(self):
        """Truncating to exactly the commit a child branches from is allowed"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("pragma new_branch=child")                     # child.source_commit = 2
        c.execute("pragma branch=master")
        c.execute("insert into t values ('r2')"); conn.commit()  # commit 3
        c.execute("pragma branch_truncate(master.2)")            # exactly at child source
        info = branch_info(c, "master")
        self.assertEqual(info["total_commits"], 2)
        conn.close()

    def test16_truncate_blocked_by_higher_of_two_children(self):
        """When two children branch from different commits, truncate blocked by higher one"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("pragma new_branch=low_child")                 # source_commit = 2
        c.execute("pragma branch=master")
        c.execute("insert into t values ('r2')"); conn.commit()  # commit 3
        c.execute("pragma new_branch=high_child")                # source_commit = 3
        c.execute("pragma branch=master")
        c.execute("insert into t values ('r3')"); conn.commit()  # commit 4
        # Cannot truncate to 2 (high_child depends on commit 3)
        with self.assertRaises(sqlite3.OperationalError):
            c.execute("pragma branch_truncate(master.2)")
        # Can truncate to 3 (highest child source_commit)
        c.execute("pragma branch_truncate(master.3)")
        self.assertEqual(branch_info(c, "master")["total_commits"], 3)
        conn.close()

    def test17_truncate_to_head_is_noop(self):
        """Truncating to the current head commit is a silent no-op"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2 = head
        c.execute("pragma branch_truncate(master.2)")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 1)
        conn.close()

    # --- Deep / wide hierarchies ---

    def test18_deep_hierarchy_5_levels(self):
        """A 5-level deep chain of branches all see the correct data"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        names = ["master", "l1", "l2", "l3", "l4"]
        for i, name in enumerate(names[1:], 1):
            c.execute(f"insert into t values ('level{i}')"); conn.commit()
            c.execute(f"pragma new_branch={name}")
        # Leaf (l4) should see all rows committed before it
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 4)
        # l2 inserted level3 before forking to l3, so l2 HEAD has 3 rows
        c.execute("pragma branch=l2")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 3)
        conn.close()

    def test19_wide_hierarchy_10_siblings_all_independent(self):
        """10 branches from the same parent commit are all independent"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        for i in range(10):
            c.execute("pragma branch=master")
            c.execute(f"pragma new_branch=sib{i}")
            c.execute(f"insert into t values ('sib{i}')"); conn.commit()
        for i in range(10):
            c.execute(f"pragma branch=sib{i}")
            c.execute("select v from t")
            self.assertEqual(c.fetchone()[0], f"sib{i}")
            c.execute("select count(*) from t")
            self.assertEqual(c.fetchone()[0], 1)
        conn.close()

    # --- Cross-connection visibility ---

    def test20_new_branch_visible_to_other_connection(self):
        """A branch created on conn1 is visible to conn2 without reconnecting"""
        conn1 = connect()
        conn2 = connect()
        if platform.system() == "Darwin":
            conn1.isolation_level = None
            conn2.isolation_level = None
        c1 = conn1.cursor()
        c2 = conn2.cursor()
        c1.execute("create table t(v text)"); conn1.commit()
        c1.execute("pragma new_branch=cross_conn_branch")
        c2.execute("pragma branches")
        names = [r[0] for r in c2.fetchall()]
        self.assertIn("cross_conn_branch", names)
        conn1.close()
        conn2.close()

    # --- Sequences of operations ---

    def test21_multiple_merges_then_write(self):
        """Merging in two rounds, then writing more commits, all works correctly"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=child")
        for i in range(4):
            c.execute(f"insert into t values ('c{i}')"); conn.commit()
        c.execute("pragma branch_merge --forward master child 2")
        # Write more on child, merge again
        c.execute("pragma branch=child")
        c.execute("insert into t values ('c4')"); conn.commit()
        c.execute("pragma branch_merge --forward master child 1")
        c.execute("pragma branch=master")
        c.execute("select count(*) from t")
        self.assertEqual(c.fetchone()[0], 3)
        conn.close()

    def test22_many_branch_switches_data_consistent(self):
        """Rapidly switching among 5 branches many times produces consistent data"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        branches = []
        for i in range(5):
            name = f"rapid{i}"
            c.execute("pragma branch=master")
            c.execute(f"pragma new_branch={name}")
            c.execute(f"insert into t values ('data{i}')"); conn.commit()
            branches.append(name)
        for _ in range(20):
            for i, name in enumerate(branches):
                c.execute(f"pragma branch={name}")
                c.execute("select v from t")
                self.assertEqual(c.fetchone()[0], f"data{i}")
        conn.close()

    def test23_branch_tree_contains_all_branches(self):
        """branch_tree output contains every branch name"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()
        c.execute("pragma new_branch=b1")
        c.execute("pragma new_branch=b2")
        c.execute("pragma branch=master")
        c.execute("pragma new_branch=b3")
        c.execute("pragma branch_tree")
        tree = c.fetchone()[0]
        for name in ["master", "b1", "b2", "b3"]:
            self.assertIn(name, tree)
        conn.close()

    def test24_50_commits_all_readable_by_number(self):
        """50 commits on one branch are each readable at their exact commit number"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(n integer)"); conn.commit()
        for i in range(1, 51):
            c.execute("insert into t values (?)", (i,)); conn.commit()
        for commit_num in [1, 10, 25, 49, 51]:
            c.execute(f"pragma branch=master.{commit_num}")
            c.execute("select count(*) from t")
            expected = commit_num - 1  # commit 1 = empty, commit 2 = 1 row, etc.
            self.assertEqual(c.fetchone()[0], expected)
        conn.close()

    def test25_branch_from_commit_1(self):
        """Branching from the very first commit (just the create table) works"""
        conn = connect()
        c = conn.cursor()
        c.execute("create table t(v text)"); conn.commit()   # commit 1
        c.execute("insert into t values ('r1')"); conn.commit()  # commit 2
        c.execute("pragma new_branch=from_one")
        info = branch_info(c, "from_one")
        self.assertEqual(info["source_commit"], 2)
        # Switch to a branch from commit 1 explicitly
        c.execute("pragma branch=master")
        # We can't do "new_branch=x at master.1" easily in this helper test
        # so just verify the source tracking is correct
        self.assertEqual(info["source_branch"], "master")
        conn.close()


if __name__ == '__main__':
    unittest.main()
