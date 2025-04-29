package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
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
    PCollection<Mutation> mutations =
        entries.apply(
            "CreateMutations",
            ParDo.of(new ObsTimeSeriesRowToMutationDoFn(cacheReader, spannerClient)));
    mutations.apply("WriteToSpanner", spannerClient.getWriteTransform());
    // writeMutationWithRedistribution(entries, cacheReader, spannerClient);
    pipeline.run();
  }

  private static void writeMutationWithRedistribution(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations",
            ParDo.of(new ObsTimeSeriesRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("redistribute", Redistribute.byKey())
        .apply(
            "ExtractMutations",
            ParDo.of(
                new DoFn<KV<String, Mutation>, Mutation>() {

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Mutation> kv, OutputReceiver<Mutation> c) {
                    var mutation = kv.getValue();
                    c.output(mutation);
                  }
                }))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }
}
