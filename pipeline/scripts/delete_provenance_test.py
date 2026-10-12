# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Checks deletion scope, preview behavior, and child-before-parent ordering."""

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import create_autospec

from delete_provenance import delete_provenance
from google.cloud.spanner_v1.database import Database


class DeleteProvenanceCliTest(unittest.TestCase):
    def test_help_displays_usage_without_required_arguments(self):
        script = Path(__file__).with_name("delete_provenance.py")
        result = subprocess.run(
            [sys.executable, "-B", str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout)
        for option in (
            "--project",
            "--instance",
            "--database",
            "--provenance",
            "--sv",
            "--execute",
        ):
            with self.subTest(option=option):
                self.assertIn(option, result.stdout)


class DeleteProvenanceTest(unittest.TestCase):
    def setUp(self):
        self._database = create_autospec(Database, instance=True)
        self._read = (
            self._database.snapshot.return_value.__enter__.return_value.execute_sql
        )
        self._read.return_value = [("sv/a", "facet/1"), ("sv/b", "facet/2")]
        self._database.execute_partitioned_dml.return_value = 0

    def test_preview_only_reads_and_reports_scope(self):
        with self.assertLogs(level="INFO") as logs:
            delete_provenance(self._database, "dc/base/example")
        self._database.execute_partitioned_dml.assert_not_called()
        self.assertIn("TimeSeriesByProvenance", self._read.call_args.args[0])
        self.assertIn(
            "WHERE provenance = @provenance",
            " ".join(self._read.call_args.args[0].split()),
        )
        self.assertEqual(
            self._read.call_args.kwargs["params"], {"provenance": "dc/base/example"}
        )
        self.assertTrue(any("sv/a (1 facets)" in line for line in logs.output))
        self.assertTrue(any("Would execute TimeSeries" in line for line in logs.output))

    def test_many_facets_use_one_array_and_all_children_precede_parents(self):
        facets = [str(i) for i in range(1000)]
        self._read.return_value = [("sv/a", facet) for facet in facets] + [
            ("sv/b", "other")
        ]
        delete_provenance(self._database, "dc/base/example", execute=True)
        calls = self._database.execute_partitioned_dml.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertTrue(
            all(
                call.args[0].startswith("DELETE FROM Observation") for call in calls[:2]
            )
        )
        expected_facets = {"sv/a": facets, "sv/b": ["other"]}
        self.assertCountEqual(
            [call.kwargs["params"]["variable"] for call in calls[:2]], expected_facets
        )
        for call in calls[:2]:
            self.assertEqual(
                " ".join(call.args[0].split()),
                "DELETE FROM Observation WHERE variable_measured = @variable "
                "AND facet_id IN UNNEST(@facets)",
            )
            params = call.kwargs["params"]
            self.assertCountEqual(params["facets"], expected_facets[params["variable"]])
        self.assertEqual(
            calls[-1].args[0], "DELETE FROM TimeSeries WHERE provenance = @provenance"
        )
        self.assertEqual(calls[-1].kwargs["params"], {"provenance": "dc/base/example"})
        for call in calls:
            self.assertFalse(call.kwargs["exclude_txn_from_change_streams"])

    def test_optional_statvars_restrict_discovery_and_parent_delete(self):
        self._read.return_value = [("sv/b", "facet/2")]
        delete_provenance(self._database, "dc/base/example", ["sv/b"], execute=True)
        calls = self._database.execute_partitioned_dml.call_args_list
        expected_params = {"provenance": "dc/base/example", "stat_vars": ["sv/b"]}
        self.assertEqual(self._read.call_args.kwargs["params"], expected_params)
        self.assertIn(
            "WHERE provenance = @provenance AND variable_measured IN UNNEST(@stat_vars)",
            " ".join(self._read.call_args.args[0].split()),
        )
        self.assertEqual(calls[0].kwargs["params"]["variable"], "sv/b")
        self.assertIn("variable_measured IN UNNEST(@stat_vars)", calls[-1].args[0])
        self.assertEqual(calls[-1].kwargs["params"], expected_params)

    def test_child_failure_prevents_parent_delete(self):
        error = RuntimeError("deadline exceeded")
        self._database.execute_partitioned_dml.side_effect = [10, error]
        with self.assertRaises(RuntimeError) as caught:
            delete_provenance(self._database, "dc/base/example", execute=True)
        self.assertIs(caught.exception, error)
        calls = self._database.execute_partitioned_dml.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn(calls[-1].kwargs["params"]["variable"], error.__notes__[0])
        self.assertTrue(
            all(call.args[0].startswith("DELETE FROM Observation") for call in calls)
        )

    def test_empty_scope_does_not_delete_parents(self):
        self._read.return_value = []
        delete_provenance(self._database, "dc/base/missing", execute=True)
        self._database.execute_partitioned_dml.assert_not_called()

    def test_failed_discovery_does_not_start_deletion(self):
        self._read.side_effect = RuntimeError("read failed")
        with self.assertRaisesRegex(RuntimeError, "read failed"):
            delete_provenance(self._database, "dc/base/example", execute=True)
        self._database.execute_partitioned_dml.assert_not_called()


if __name__ == "__main__":
    unittest.main()
