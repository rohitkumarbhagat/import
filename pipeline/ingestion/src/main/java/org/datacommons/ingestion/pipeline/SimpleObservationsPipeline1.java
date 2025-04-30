package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.common.base.Joiner;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationKVDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ObsTimeSeriesRowToMutationDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ObsTimeSeriesRowToMutationGroupDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ObsTimeSeriesRowToMutationKVDoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimpleObservationsPipeline is a pipeline that reads a single import group from cache and
 * populates the spanner observations table.
 */
public class SimpleObservationsPipeline1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleObservationsPipeline1.class);

  public static void main(String[] args) {
    SimpleObservationsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SimpleObservationsPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info(
        "Running simple observations pipeline for import group: {}", options.getImportGroup());

    CacheReader cacheReader = new CacheReader(List.of());
    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId(options.getProjectId())
            .spannerInstanceId(options.getSpannerInstanceId())
            .spannerDatabaseId(options.getSpannerDatabaseId())
            .nodeTableName(options.getSpannerNodeTableName())
            .edgeTableName(options.getSpannerEdgeTableName())
            .observationTableName(options.getSpannerObservationTableName())
            .build();

    String cachePath =
        CacheReader.getCachePath(
            options.getProjectId(), options.getStorageBucketId(), options.getImportGroup());

    PCollection<String> entries = pipeline.apply("ReadFromCache", TextIO.read().from(cachePath));
    // writeMutations(entries, cacheReader, spannerClient);
    writeMutationWithGBK(entries, cacheReader, spannerClient);
    pipeline.run();
  }

  private static void writeMutations(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<Mutation> mutations =
        entries.apply(
            "CreateMutations",
            ParDo.of(new ObsTimeSeriesRowToMutationDoFn(cacheReader, spannerClient)));
    mutations.apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationWithGBK(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations",
            ParDo.of(new ObsTimeSeriesRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("GBK", GroupByKey.create())
        .apply(
            "ExtractMutations",
            ParDo.of(
                new DoFn<KV<String, Iterable<Mutation>>, Mutation>() {

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<Mutation>> kv, OutputReceiver<Mutation> c) {

                    var duplicateCount = 0;
                    // sort the values by key
                    var sortedMutations =
                        StreamSupport.stream(kv.getValue().spliterator(), false)
                            .sorted(Comparator.comparing(SimpleObservationsPipeline1::getKey))
                            .toList();
                    var set = new HashSet<>();
                    for (Mutation mutation : sortedMutations) {
                      if (set.add(getKey(mutation))) {
                        c.output(mutation);
                      } else {
                        duplicateCount++;
                      }
                    }
                    if (duplicateCount > 0) {
                      LOGGER.info(
                          "Key=[{}]:duplicate mutations count=[{}]", kv.getKey(), duplicateCount);
                    }
                  }
                }))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static String getKey(Mutation m) {
    var mutationMap = m.asMap();
    String variableMeasured = mutationMap.get("variable_measured").getString();
    String observationAbout = mutationMap.get("observation_about").getString();
    var provenance = mutationMap.get("provenance").getString();
    var observationPeriod = mutationMap.get("observation_period").getString();
    var measurementMethod = mutationMap.get("measurement_method").getString();
    var unit = mutationMap.get("unit").getString();
    var scalingFactor = mutationMap.get("scaling_factor").getString();

    return Joiner.on("::")
        .join(
            variableMeasured,
            observationAbout,
            provenance,
            observationPeriod,
            measurementMethod,
            unit,
            scalingFactor);
  }
}
