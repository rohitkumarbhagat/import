# /// script
# requires-python = ">=3.11"
# dependencies = ["google-cloud-spanner"]
# ///
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

r"""Delete Observation and TimeSeries rows for one provenance in Spanner.

By default, the script previews deletes without changing data. Add
--execute to delete rows. Use --sv DCID to select a statvar, and repeat
--sv to select more than one.

Example preview from the import repository root:

    uv run pipeline/scripts/delete_provenance.py \
        --project datcom-store \
        --instance dc-graph-staging \
        --database dc_graph \
        --provenance dc/base/EPACriteriaAirPollutants

Requires Python 3.11 or later and Application Default Credentials.
For deletion, each selected (variable_measured, facet_id) pair must belong
only to the specified provenance. The script does not check ownership.
Other jobs must not write these rows while deletion runs.

See pipeline/scripts/README.md for the procedure, failure recovery, and tests.
"""

import argparse
import logging
import time
from collections import defaultdict

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types

LOGGER = logging.getLogger(__name__)


def delete_provenance(database, provenance, stat_vars=None, execute=False):
    """Preview Observation and TimeSeries deletes for a provenance.

    Set execute=True to delete rows. If stat_vars is empty or None, include
    all statvars for the provenance.
    """
    predicate = "provenance = @provenance"
    params = {"provenance": provenance}
    types = {"provenance": param_types.STRING}
    if stat_vars:
        predicate += " AND variable_measured IN UNNEST(@stat_vars)"
        params["stat_vars"] = stat_vars
        types["stat_vars"] = param_types.Array(param_types.STRING)

    LOGGER.info(
        "Discovering existing facets for %s; statvars=%s",
        provenance,
        stat_vars or "all",
    )
    facets_by_variable = defaultdict(list)
    with database.snapshot() as snapshot:
        rows = snapshot.execute_sql(
            "SELECT DISTINCT variable_measured, facet_id "
            "FROM TimeSeries@{FORCE_INDEX=TimeSeriesByProvenance} "
            f"WHERE {predicate}",
            params=params,
            param_types=types,
            request_options={"priority": "PRIORITY_LOW"},
        )
        for variable, facet in rows:
            facets_by_variable[variable].append(facet)

    if not facets_by_variable:
        LOGGER.info("No matching TimeSeries rows; nothing to delete.")
        return

    deletes = []
    for variable, facets in sorted(facets_by_variable.items()):
        deletes.append(
            (
                f"Observation: {variable} ({len(facets)} facets)",
                (
                    "DELETE FROM Observation WHERE variable_measured = @variable "
                    "AND facet_id IN UNNEST(@facets)"
                ),
                {"variable": variable, "facets": sorted(facets)},
                {
                    "variable": param_types.STRING,
                    "facets": param_types.Array(param_types.STRING),
                },
            )
        )

    # Delete TimeSeries last to avoid cascading over remaining Observation rows.
    # A rerun also needs TimeSeries rows to discover the facets again.
    deletes.append(
        ("TimeSeries", f"DELETE FROM TimeSeries WHERE {predicate}", params, types)
    )
    LOGGER.info(
        "%s %d Observation statements, then 1 TimeSeries statement.",
        "Executing" if execute else "Preview:",
        len(facets_by_variable),
    )
    for label, sql, statement_params, statement_types in deletes:
        LOGGER.info("%s %s", "Executing" if execute else "Would execute", label)
        LOGGER.info("SQL: %s", sql)
        if not execute:
            continue
        start = time.monotonic()
        try:
            row_count = database.execute_partitioned_dml(
                sql,
                params=statement_params,
                param_types=statement_types,
                request_options={"priority": "PRIORITY_LOW"},
                exclude_txn_from_change_streams=False,
            )
        except Exception as exc:
            exc.add_note(
                f"Deleting {label} for provenance {provenance}. "
                "Earlier deletes may have committed; rerun the same scope."
            )
            raise
        LOGGER.info(
            "%s completed in %.1fs; deleted at least %d rows.",
            label,
            time.monotonic() - start,
            row_count,
        )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--provenance",
        required=True,
        help="Exact stored provenance, e.g. dc/base/EPACriteriaAirPollutants",
    )
    parser.add_argument(
        "--sv",
        action="append",
        dest="stat_vars",
        metavar="DCID",
        help="Restrict to this statvar; repeat for multiple statvars",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute deletes; otherwise only read facets and preview",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    client = spanner.Client(project=args.project)
    database = client.instance(args.instance).database(args.database)
    LOGGER.info("Target: %s", database.name)
    try:
        delete_provenance(database, args.provenance, args.stat_vars, args.execute)
    finally:
        database.close()


if __name__ == "__main__":
    main()
