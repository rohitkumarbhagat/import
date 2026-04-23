package org.datacommons.ingestion.timeseries;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.datacommons.Storage.Observations;

/** Row shapes used by the timeseries backfill. */
final class SourceObservationRows {
  private SourceObservationRows() {}

  static SourceObservationRow toObservationRow(Struct row) {
    String observationAbout = row.getString("observation_about");
    String variableMeasured = row.getString("variable_measured");
    String facetId = row.getString("facet_id");
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            observationAbout,
            variableMeasured,
            facetId,
            getNullableString(row, "observation_period"),
            getNullableString(row, "measurement_method"),
            getNullableString(row, "unit"),
            getNullableString(row, "scaling_factor"),
            getNullableString(row, "import_name"),
            getNullableString(row, "provenance_url"),
            !row.isNull("is_dc_aggregate") && row.getBoolean("is_dc_aggregate"),
            row.getString("provenance"));
    List<SourcePointRow> pointRows = new ArrayList<>();
    for (Map.Entry<String, String> entry : parseObservations(row).getValuesMap().entrySet()) {
      pointRows.add(
          new SourcePointRow(
              observationAbout, variableMeasured, facetId, entry.getKey(), entry.getValue()));
    }
    return new SourceObservationRow(seriesRow, pointRows);
  }

  private static String getNullableString(Struct row, String columnName) {
    return row.isNull(columnName) ? "" : row.getString(columnName);
  }

  private static Observations parseObservations(Struct row) {
    if (row.isNull("observations")) {
      return Observations.getDefaultInstance();
    }
    ByteArray protoBytes = row.getBytes("observations");
    try {
      return Observations.parseFrom(protoBytes.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse observations proto", e);
    }
  }
}

record SourceObservationRow(SourceSeriesRow seriesRow, List<SourcePointRow> pointRows) {}

record SourceSeriesRow(
    String observationAbout,
    String variableMeasured,
    String facetId,
    String observationPeriod,
    String measurementMethod,
    String unit,
    String scalingFactor,
    String importName,
    String provenanceUrl,
    boolean isDcAggregate,
    String provenance) {}

record SourcePointRow(
    String observationAbout, String variableMeasured, String facetId, String date, String value) {}
