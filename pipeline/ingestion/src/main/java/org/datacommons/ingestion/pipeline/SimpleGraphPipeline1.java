package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationKVDoFn;
import org.datacommons.ingestion.pipeline.Transforms.LazyArcRowToMutationGroupDoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
    // -----Ignore----------
    // write mutation groups
    // writeAsMutationGroups(entries, cacheReader, spannerClient);
    //  write direct entities
    // writeAsMutations(entries, cacheReader, spannerClient);
    // writeMutationsWithExecutionPerPartition(entries, cacheReader, spannerClient);
    // writeMutationsWithWindowingAndRedistribution(entries, cacheReader, spannerClient);
    // writeMutationsWithWindowingAndGroupBy(entries, cacheReader, spannerClient);
    // writeNodesDedupedAfterPartition(entries, cacheReader, spannerClient);
    // ---- Ignore ----------

    // Use one of the following pipelines

    // 1. Redistribute and write nodes+edges together
    // writeMutationsWithRedistribution(entries, cacheReader, spannerClient);
    writeMutationsWithRedistributionAndSampling(entries, cacheReader, spannerClient);

    // 2. Redistribute nodes only. Partition edges. Write them separately
    // writeNodeRedistributedEdgesPartitioned(entries, cacheReader, spannerClient);
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
        .apply("ExtractMutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithRedistributionAndSampling(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    // sampling any() uses GBK
    // generate
    mutations
        //
        .apply("sample mutations", ParDo.of(new SampleMutationDoFn(1000)))
        // .apply("Extract sample Mutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("Write sample ToSpanner", spannerClient.getWriteTransform());
    mutations
        .apply("redistribute", Redistribute.byKey())
        .apply("ExtractMutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithWindowingAndRedistribution(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    var windowSeconds = 1 * 60;
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("add timestamp", WithTimestamps.of(m -> Instant.now()))
        .apply(
            "window",
            Window.<KV<String, Mutation>>into(
                    FixedWindows.of(Duration.standardSeconds(windowSeconds)))
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(10))))
                .withAllowedLateness(Duration.standardMinutes(2))
                .discardingFiredPanes())
        .apply("redistribute", Redistribute.byKey())
        .apply("ExtractMutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithWindowingAndGroupBy(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    var windowSeconds = 1 * 60l;
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    mutations
        .apply("add timestamp", WithTimestamps.of(m -> Instant.now()))
        .apply(
            "window",
            Window.<KV<String, Mutation>>into(
                    FixedWindows.of(Duration.standardSeconds(windowSeconds)))
                .triggering(
                    // Repeatedly.forever(
                    //     AfterProcessingTime.pastFirstElementInPane()
                    //         .plusDelayOf(Duration.standardSeconds(10))))
                    Repeatedly.forever(AfterPane.elementCountAtLeast(100)))
                .withAllowedLateness(Duration.standardMinutes(2))
                .discardingFiredPanes())
        .apply("GroupByKey", GroupByKey.create())
        .apply(
            "ExtractMutations",
            ParDo.of(
                new DoFn<KV<String, Iterable<Mutation>>, Mutation>() {
                  final Set<String> nodeIds = new HashSet<>();

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<Mutation>> kv, OutputReceiver<Mutation> c) {
                    var mutations = kv.getValue();
                    for (Mutation mutation : mutations) {
                      // Key can be different from subject
                      var subjectId = mutation.asMap().get("subject_id").getString();
                      if (mutation.getTable().equals(spannerClient.getNodeTableName())
                          && nodeIds.contains(subjectId)) {
                        continue;
                      }
                      c.output(mutation);
                      nodeIds.add(subjectId);
                    }
                  }
                }))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithPartitionBy(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    PCollectionList<KV<String, Mutation>> mutationPartitions =
        mutations.apply(partitionMutations(10));
    mutationPartitions
        .apply(Flatten.<KV<String, Mutation>>pCollections())
        .apply("ExtractMutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("WriteToSpanner", spannerClient.getWriteTransform());
  }

  private static void writeMutationsWithExecutionPerPartition(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    int partition_count = 5;
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));
    PCollectionList<KV<String, Mutation>> mutationPartitions =
        mutations.apply(partitionMutations(partition_count));
    int index = 0;
    for (PCollection<KV<String, Mutation>> p : mutationPartitions.getAll()) {
      index++;
      // p.apply(Redistribute.byKey())
      p.apply("ExtractMutations_" + index, ParDo.of(new ExtractMutationDoFn(spannerClient)))
          .apply("WriteToSpanner_" + index, spannerClient.getWriteTransform());
    }
  }

  private static void writeNodeRedistributedEdgesPartitioned(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    int partition_count = 5;
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));

    // separate edges and nodes mutations
    PCollection<KV<String, Mutation>> nodeMutations =
        mutations.apply(
            "FilterNodeMutations", filterMutationsForTable(spannerClient.getNodeTableName()));
    PCollection<KV<String, Mutation>> edgeMutations =
        mutations.apply(
            "FilterEdgeMutations", filterMutationsForTable(spannerClient.getEdgeTableName()));

    // redistribute and write nodes
    nodeMutations
        .apply("redistribute", Redistribute.byKey())
        .apply("ExtractNodeMutations", ParDo.of(new ExtractMutationDoFn(spannerClient)))
        .apply("WriteNodesToSpanner", spannerClient.getWriteTransform());

    // write edges with partiiton by subject_id

    PCollectionList<KV<String, Mutation>> mutationPartitions =
        edgeMutations.apply(partitionMutations(partition_count));
    int index = 0;
    for (PCollection<KV<String, Mutation>> p : mutationPartitions.getAll()) {
      index++;
      // p.apply(Redistribute.byKey())
      p.apply("ExtractEdgeMutations_" + index, ParDo.of(new ExtractMutationDoFn(spannerClient)))
          .apply("WriteEdgesToSpanner_" + index, spannerClient.getWriteTransform());
    }
  }

  @NotNull
  private static SingleOutput<KV<String, Mutation>, KV<String, Mutation>> filterMutationsForTable(
      String tableName) {
    return ParDo.of(
        new DoFn<KV<String, Mutation>, KV<String, Mutation>>() {
          @ProcessElement
          public void processElement(
              @Element KV<String, Mutation> kv, OutputReceiver<KV<String, Mutation>> c) {
            if (kv.getValue().getTable().equals(tableName)) {
              c.output(kv);
            }
          }
        });
  }

  private static void writeNodesDedupedAfterPartition(
      PCollection<String> entries, CacheReader cacheReader, SpannerClient spannerClient) {
    int partition_count = 5;
    PCollection<KV<String, Mutation>> mutations =
        entries.apply(
            "CreateMutations", ParDo.of(new ArcRowToMutationKVDoFn(cacheReader, spannerClient)));

    // separate edges and nodes mutations
    PCollection<KV<String, Mutation>> nodeMutations =
        mutations
            .apply("FilterNodeMutations", filterMutationsForTable(spannerClient.getNodeTableName()))
            .apply(
                "redistribute arbitarily node mutations",
                Redistribute.arbitrarily()); // to avoid fusion
    PCollection<KV<String, Mutation>> edgeMutations =
        mutations.apply(
            "FilterEdgeMutations", filterMutationsForTable(spannerClient.getEdgeTableName()));

    // Process Node partitions
    PCollectionList<KV<String, Mutation>> nodePartitions =
        nodeMutations.apply("Partition Nodes", partitionMutations(partition_count));
    int node_partition_index = 0;
    for (PCollection<KV<String, Mutation>> p : nodePartitions.getAll()) {
      node_partition_index++;
      // p.apply(Redistribute.byKey())
      p.apply(
              "ExtractNodeMutations_" + node_partition_index,
              ParDo.of(new ExtractMutationDoFn(spannerClient)))
          .apply("WriteNodesToSpanner_" + node_partition_index, spannerClient.getWriteTransform());
    }

    // Process Edge partitions
    // write edges with partiiton by subject_id

    PCollectionList<KV<String, Mutation>> edgePartitions =
        edgeMutations.apply("partition edges", partitionMutations(partition_count));
    int edge_partition_index = 0;
    for (PCollection<KV<String, Mutation>> p : edgePartitions.getAll()) {
      edge_partition_index++;
      // p.apply(Redistribute.byKey())
      p.apply(
              "ExtractEdgeMutations_" + edge_partition_index,
              ParDo.of(new ExtractMutationDoFn(spannerClient)))
          .apply("WriteEdgesToSpanner_" + edge_partition_index, spannerClient.getWriteTransform());
    }
  }

  @NotNull
  private static Partition<KV<String, Mutation>> partitionMutations(int partition_count) {
    return Partition.of(
        partition_count,
        new PartitionFn<KV<String, Mutation>>() {
          public int partitionFor(KV<String, Mutation> kv, int numPartitions) {
            // var subjectId = kv.getValue().asMap().get("subject_id").getString();
            // var key = subjectId.substring(0, Math.min(subjectId.length(), 15));
            var key = kv.getKey();
            return Math.abs(key.hashCode()) % numPartitions;
          }
        });
  }

  static class ExtractMutationDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final SpannerClient spannerClient;
    private Set<String> nodeIds = null;

    public ExtractMutationDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @DoFn.FinishBundle
    public synchronized void FinishBundle(FinishBundleContext c) throws Exception {
      // Clear older nodeIds, in case same object is being reused for other bundle.
      this.nodeIds = null;
    }

    @DoFn.StartBundle
    public synchronized void StartBundle(StartBundleContext c) throws Exception {
      this.nodeIds = Sets.newConcurrentHashSet();
    }

    @ProcessElement
    public void processElement(@Element KV<String, Mutation> kv, OutputReceiver<Mutation> c) {
      var mutation = kv.getValue();
      // Key can be different from subject
      var subjectId = mutation.asMap().get("subject_id").getString();
      if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
        if (nodeIds.contains(subjectId)) {
          return;
        }
        // Add only for Node mutation.
        nodeIds.add(subjectId);
      }
      c.output(mutation);
      // nodeIds.add(subjectId);
    }
  }

  static class SampleMutationDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final AtomicInteger count = new AtomicInteger(0);
    private final int sample_limit;

    public SampleMutationDoFn(int sample_limit) {
      this.sample_limit = sample_limit;
    }

    @DoFn.FinishBundle
    public synchronized void FinishBundle(FinishBundleContext c) throws Exception {
      // Clear older nodeIds, in case same object is being reused for other bundle.
      this.count.set(0);
    }

    @DoFn.StartBundle
    public synchronized void StartBundle(StartBundleContext c) throws Exception {
      this.count.set(0);
    }

    @ProcessElement
    public void processElement(@Element KV<String, Mutation> kv, OutputReceiver<Mutation> c) {
      var mutation = kv.getValue();
      if(count.getAndIncrement()< sample_limit){
        c.output(mutation);
      }
    }
  }
}
