package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationGroupDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationKVDoFn;
import org.datacommons.ingestion.pipeline.Transforms.LazyArcRowToMutationGroupDoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimpleGraphPipeline is a pipeline that reads a single import group from cache and populates the
 * spanner graph tables (nodes and edges) simultaneously. DO NOT use this pipeline if you want to
 * write nodes and edges separately or in a certain order.
 */
public class SimpleGraphPipeline1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleGraphPipeline.class);

  public static void main(String[] args) {
    SimpleGraphPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SimpleGraphPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info("Running simple graph pipeline for import group: {}", options.getImportGroup());

    CacheReader cacheReader = new CacheReader(options.getSkipPredicatePrefixes());
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
    // write mutation groups
    // writeAsMutationGroups(entries, cacheReader, spannerClient);
    //  write direct entities
    // writeAsMutations(entries, cacheReader, spannerClient);
    writeMutationsWithRedistribution(entries, cacheReader, spannerClient);

    pipeline.run();
  }

  private static void writeAsMutationGroups(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<MutationGroup> mutationGroups =
        entries.apply(
            "CreateMutationGroups",
            ParDo.of(new LazyArcRowToMutationGroupDoFn(cacheReader, spannerClient)));
    mutationGroups.apply("WriteToSpanner", spannerClient.getWriteGroupedTransform());
  }

  private static void writeAsMutations(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<Mutation> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationDoFn(cacheReader, spannerClient)));
    mutations.apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithRedistribution(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("redistribute", Redistribute.byKey())
        .apply(
            "ExtractMutations",
            ParDo.of(
                new DoFn<KV<String, Mutation>, Mutation>() {
                  final Set<String> nodeIds = new HashSet<>();

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Mutation> kv, OutputReceiver<Mutation> c) {
                    var mutation = kv.getValue();
                    var subjectId = kv.getKey();
                    if (mutation.getTable().equals(spannerClient.getNodeTableName())
                        && nodeIds.contains(subjectId)) {
                      return;
                    }
                    c.output(mutation);
                    nodeIds.add(subjectId);
                  }
                }))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithGroupBy(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("GroupByKey", GroupByKey.create())
        .apply(
            "ExtractMutationsFromGroup",
            ParDo.of(
                new DoFn<KV<String, Iterable<Mutation>>, Mutation>() {
                  final Set<String> nodeIds = new HashSet<>();

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<Mutation>> kv, OutputReceiver<Mutation> c) {
                    var mutations = kv.getValue();
                    var subjectId = kv.getKey();
                    for (Mutation mutation : mutations) {
                      if (mutation.getTable().equals(spannerClient.getNodeTableName())
                          && nodeIds.contains(subjectId)) {
                        return;
                      }
                      c.output(mutation);
                      nodeIds.add(subjectId);
                    }
                  }
                }))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }
}
