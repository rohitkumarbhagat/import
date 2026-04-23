package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import java.util.Comparator;
import java.util.List;
import org.datacommons.Storage.Observations;
import org.junit.Test;

public class SourceObservationRowsTest {
  @Test
  public void toObservationRow_expandsObservationProto() {
    Struct row =
        Struct.newBuilder()
            .set("observation_about")
            .to("geoId/06")
            .set("variable_measured")
            .to("Count_Person")
            .set("facet_id")
            .to("123")
            .set("observation_period")
            .to((String) null)
            .set("measurement_method")
            .to((String) null)
            .set("unit")
            .to((String) null)
            .set("scaling_factor")
            .to((String) null)
            .set("import_name")
            .to((String) null)
            .set("provenance_url")
            .to((String) null)
            .set("is_dc_aggregate")
            .to(false)
            .set("provenance")
            .to("dc/base/TestImport")
            .set("observations")
            .to(
                ByteArray.copyFrom(
                    Observations.newBuilder()
                        .putValues("2023", "1")
                        .putValues("2024", "2")
                        .build()
                        .toByteArray()))
            .build();

    SourceObservationRow observationRow = SourceObservationRows.toObservationRow(row);
    List<SourcePointRow> rows = observationRow.pointRows();
    rows.sort(Comparator.comparing(SourcePointRow::date));

    assertEquals("geoId/06", observationRow.seriesRow().observationAbout());
    assertEquals("Count_Person", observationRow.seriesRow().variableMeasured());
    assertEquals(2, rows.size());
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2023", "1"), rows.get(0));
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2024", "2"), rows.get(1));
  }
}
