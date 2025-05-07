package org.datacommons.ingestion.pipeline;

import static org.datacommons.ingestion.pipeline.SkipProcessing.SKIP_GRAPH;
import static org.datacommons.ingestion.pipeline.SkipProcessing.SKIP_OBS;

import com.google.cloud.spanner.Mutation;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.ImportGroupVersions;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.pipeline.Transforms.CacheRowMutationsDoFn;
import org.datacommons.ingestion.pipeline.Transforms.ExtractGraphMutationsDoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {
  private static final Logger LOGGER = LoggerFactory.getLogger(Transforms.class);

  /**
   * DoFn that outputs observation and graph (nodes and edges) mutations for a single cache row.
   * Both are output as KV<String, Mutation> objects. Observation mutations are keyed by variable.
   * Graph mutations are keyed by subject ID.
   */
  static class CacheRowMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private static final Counter seenNodesCounter =
        Metrics.counter(CacheRowMutationsDoFn.class, "seenNodes");
    static final Counter seenEdgesCounter =
        Metrics.counter(CacheRowMutationsDoFn.class, "seenEdges");

    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final TupleTag<KV<String, Mutation>> graph;
    private final TupleTag<Mutation> observations;
    private final SkipProcessing skipProcessing;
    private final Set<String> seenNodes = new HashSet<>();
    private final Set<String> seenEdges = Sets.newHashSet();

    private CacheRowMutationsDoFn(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        SkipProcessing skipProcessing,
        TupleTag<KV<String, Mutation>> graph,
        TupleTag<Mutation> observations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
      this.graph = graph;
      this.observations = observations;
    }

    @StartBundle
    public void startBundle() {
      seenNodes.clear();
      seenEdges.clear();
    }

    @FinishBundle
    public void finishBundle() {
      seenNodes.clear();
      seenEdges.clear();
    }

    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      if (CacheReader.isArcCacheRow(row) && skipProcessing != SKIP_GRAPH) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        var kvs = spannerClient.toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient
            .filterGraphKVMutations(kvs, seenNodes, seenEdges, seenNodesCounter, seenEdgesCounter)
            .forEach(out.get(graph)::output);

      } else if (CacheReader.isObsTimeSeriesCacheRow(row) && skipProcessing != SKIP_OBS) {
        var obs = cacheReader.parseTimeSeriesRow(row);
        spannerClient.toObservationMutations(obs).forEach(out.get(observations)::output);
      }
    }
  }

  static class CacheRowKVMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final TupleTag<KV<String, Mutation>> graph;
    private final TupleTag<KV<String, Mutation>> observations;
    private final SkipProcessing skipProcessing;
    private final Set<String> seenNodes = new HashSet<>();

    private CacheRowKVMutationsDoFn(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        SkipProcessing skipProcessing,
        TupleTag<KV<String, Mutation>> graph,
        TupleTag<KV<String, Mutation>> observations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
      this.graph = graph;
      this.observations = observations;
    }

    @StartBundle
    public void startBundle() {
      seenNodes.clear();
    }

    @FinishBundle
    public void finishBundle() {
      seenNodes.clear();
    }

    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      if (CacheReader.isArcCacheRow(row) && skipProcessing != SKIP_GRAPH) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        var kvs = spannerClient.toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient
            .filterGraphKVMutations(kvs, seenNodes, null, null, null)
            .forEach(out.get(graph)::output);
      } else if (CacheReader.isObsTimeSeriesCacheRow(row) && skipProcessing != SKIP_OBS) {
        var obs = cacheReader.parseTimeSeriesRow(row);
        spannerClient.toObservationKVMutations(obs).forEach(out.get(observations)::output);
      }
    }
  }

  static class ExtractGraphMutationsDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = new HashSet<>();
    private final Set<String> seenEdges = Sets.newHashSet();

    public ExtractGraphMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {
      this.seenNodes.clear();
      this.seenEdges.clear();
    }

    @FinishBundle
    public void finishBundle() {
      this.seenNodes.clear();
      this.seenEdges.clear();
    }

    @ProcessElement
    public void processElement(@Element KV<String, Mutation> kv, OutputReceiver<Mutation> out) {
      var mutation = kv.getValue();
      var subjectId = SpannerClient.getSubjectId(mutation);
      if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
        if (seenNodes.contains(subjectId)) {
          return;
        }
        seenNodes.add(subjectId);
      }
      if (mutation.getTable().equals(spannerClient.getEdgeTableName())) {
        var edgeKey = SpannerClient.getEdgeKey(mutation);
        if (seenEdges.contains(edgeKey)) {
          return;
        }
        seenEdges.add(edgeKey);
      }
      out.output(mutation);
    }
  }

  static class ExtractGraphKVMutationsDoFn extends DoFn<KV<String, Iterable<Mutation>>, Mutation> {
    private static final int MAX_LOG_COUNT = 1000;
    private final SpannerClient spannerClient;
    static Counter duplicateNodesCounter =
        Metrics.counter(ExtractGraphKVMutationsDoFn.class, "duplicateEdges");
    static Counter duplicateEdgesCounter =
        Metrics.counter(ExtractGraphKVMutationsDoFn.class, "duplicateNodes");

    int logCount = 0;

    // private final Set<String> seenNodes = new HashSet<>();
    // private final Set<String> seenEdges = Sets.newHashSet();

    public ExtractGraphKVMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {

      logCount = 0;
      // this.seenNodes.clear();
      // this.seenEdges.clear();
    }

    @FinishBundle
    public void finishBundle() {
      logCount = 0;
      // this.seenNodes.clear();
      // this.seenEdges.clear();
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Mutation>> kvs, OutputReceiver<Mutation> out) {
      final Set<String> seenNodes = new HashSet<>();
      final Set<String> seenEdges = Sets.newHashSet();
      // count duplicate nodes and edges
      var duplicateNodes = 0;
      var duplicateEdges = 0;
      for (var mutation : kvs.getValue()) {
        var subjectId = SpannerClient.getSubjectId(mutation);
        if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
          if (seenNodes.contains(subjectId)) {
            duplicateNodes++;
            duplicateNodesCounter.inc();
            continue;
          }
          seenNodes.add(subjectId);
        }
        if (mutation.getTable().equals(spannerClient.getEdgeTableName())) {
          var edgeKey = SpannerClient.getEdgeKey(mutation);
          if (seenEdges.contains(edgeKey)) {
            if (logCount < MAX_LOG_COUNT) {
              LOGGER.info("Duplicate edge key=[{}]", edgeKey);
              logCount++;
            }
            duplicateEdgesCounter.inc();
            duplicateEdges++;
            continue;
          }
          seenEdges.add(edgeKey);
        }
        out.output(mutation);
      }

      // if (duplicateNodes + duplicateEdges > 0) {
      //   LOGGER.info(
      //       "Key=[{}]:duplicate nodes count=[{}], duplicate edges count=[{}]",
      //       kvs.getKey(),
      //       duplicateNodes,
      //       duplicateEdges);
      // }
    }
  }

  static class ExtractMutationsDoFn extends DoFn<KV<String, Iterable<Mutation>>, Mutation> {
    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = new HashSet<>();

    public ExtractMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {
      this.seenNodes.clear();
    }

    @FinishBundle
    public void finishBundle() {
      this.seenNodes.clear();
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Mutation>> kvs, OutputReceiver<Mutation> out) {
      kvs.getValue()
          .forEach(
              mutation -> {
                if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
                  var subjectId = SpannerClient.getSubjectId(mutation);
                  if (seenNodes.contains(subjectId)) {
                    return;
                  }
                  seenNodes.add(subjectId);
                }
                out.output(mutation);
              });
    }
  }

  static class ImportGroupTransform extends PTransform<PCollection<String>, PCollection<Void>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final SkipProcessing skipProcessing;
    private final String importGroupName;

    public ImportGroupTransform(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        SkipProcessing skipProcessing,
        String importGroupName) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
      this.importGroupName = importGroupName;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> cacheRows) {
      TupleTag<Mutation> observations = new TupleTag<>() {};
      TupleTag<KV<String, Mutation>> graph = new TupleTag<>() {};
      CacheRowMutationsDoFn createMutations =
          new CacheRowMutationsDoFn(
              cacheReader, spannerClient, skipProcessing, graph, observations);

      PCollectionTuple mutations =
          cacheRows.apply(
              "CreateMutations",
              ParDo.of(createMutations).withOutputTags(graph, TupleTagList.of(observations)));

      // Write graph to spanner.
      var graphResult =
          mutations
              .get(graph)
              // .apply("RedistributeGraphMutations", Redistribute.byKey())
              // .apply(
              //     "ExtractGraphMutations", ParDo.of(new
              // ExtractGraphMutationsDoFn(spannerClient)))
              .apply("GBK graph", GroupByKey.create())
              .apply(
                  "ExtractGraphKVMutations",
                  ParDo.of(new ExtractGraphKVMutationsDoFn(spannerClient)))
              .apply("WriteGraphToSpanner", spannerClient.getWriteTransformForGraph());

      // Write observations to spanner.
      var obsResult =
          mutations
              .get(observations)
              .apply(
                  "WriteObsToSpanner",
                  Objects.equals(importGroupName, "ipcc")
                      ? spannerClient.getWriteTransformForIpcc()
                      : spannerClient.getWriteTransform());

      var writeResults = PCollectionList.of(graphResult.getOutput()).and(obsResult.getOutput());
      // var writeResults = PCollectionList.of(obsResult.getOutput());

      return writeResults.apply("Done", Flatten.pCollections());
    }
  }

  static PCollection<Void> buildImportGroupPipeline(
      Pipeline pipeline,
      PCollection<Void> parent,
      String importGroupVersion,
      CacheReader cacheReader,
      SpannerClient spannerClient) {
    var options = pipeline.getOptions().as(IngestionPipelineOptions.class);
    var importGroupName = ImportGroupVersions.getImportGroupName(importGroupVersion);
    var importGroupFilePath = cacheReader.getImportGroupCachePath(importGroupVersion);
    var read = pipeline.apply("Read: " + importGroupName, TextIO.read().from(importGroupFilePath));
    if (parent != null) {
      read = read.apply("wait for parent", Wait.on(parent));
    }
    return read.apply(
        "Ingest: " + importGroupName,
        // new Transforms.ImportGroupKVTransform(
        //     cacheReader, spannerClient, options.getSkipProcessing()));
        new Transforms.ImportGroupTransform(
            cacheReader, spannerClient, options.getSkipProcessing(), importGroupName));
  }

  static void buildIngestionPipeline(
      Pipeline pipeline,
      List<String> importGroupVersions,
      CacheReader cacheReader,
      SpannerClient spannerClient) {

    //  ****** uscensus first-> ipcc
    // PCollection<Void> uscensusOutput = null;
    // // filter all imports groups except from starting with ipcc into another array
    // List<String> ipccImportGroupVersions =
    //     importGroupVersions.stream().filter(s -> s.startsWith("ipcc")).toList();
    // List<String> nonIpccImportGroupVersions =
    //     importGroupVersions.stream().filter(s -> !s.startsWith("ipcc")).toList();
    // for (var importGroupVersion : nonIpccImportGroupVersions) {
    //   LOGGER.info("******* processing importGroupVersion = " + importGroupVersion);
    //   // non-ipcc runs in parallel
    //   var output =
    //       buildImportGroupPipeline(pipeline, null, importGroupVersion, cacheReader,
    // spannerClient);
    //   if (importGroupVersion.startsWith("uscensus")) {
    //     uscensusOutput = output;
    //   }
    // }
    // for (var importGroupVersion : ipccImportGroupVersions) {
    //   LOGGER.info("******* processing importGroupVersion = " + importGroupVersion);
    //   // ipcc ones start after uscensus has ended
    //   buildImportGroupPipeline(
    //       pipeline, uscensusOutput, importGroupVersion, cacheReader, spannerClient);
    // }

    //  ****** ipcc first-> uscensus
    // PCollection<Void> ipccOutput = null;
    // List<String> uscensusImportGroupVersions =
    //     importGroupVersions.stream().filter(s -> s.startsWith("12345678uscensus")).toList();
    // List<String> nonUscensusImportGroupVersions =
    //     importGroupVersions.stream().filter(s -> !s.startsWith("12345678uscensus")).toList();
    // for (var importGroupVersion : nonUscensusImportGroupVersions) {
    //   LOGGER.info("******* processing importGroupVersion = " + importGroupVersion);
    //   // non-ipcc runs in parallel
    //   var output =
    //       buildImportGroupPipeline(pipeline, null, importGroupVersion, cacheReader,
    // spannerClient);
    //   if (importGroupVersion.startsWith("123456ipcc")) {
    //     ipccOutput = output;
    //   }
    // }
    // for (var importGroupVersion : uscensusImportGroupVersions) {
    //   LOGGER.info("******* processing importGroupVersion = " + importGroupVersion);
    //   // ipcc ones start after uscensus has ended
    //   buildImportGroupPipeline(
    //       pipeline, ipccOutput, importGroupVersion, cacheReader, spannerClient);
    // }

    // importGroupVersions.sort(
    //     Comparator.comparing(
    //         (importGroup) -> {
    //           if (importGroup.startsWith("uscensus")) {
    //             return 0;
    //           }
    //           if (importGroup.startsWith("ipcc")) {
    //             return 10;
    //           }
    //           return 0;
    //         }));
    // // (a, b) -> {
    // //   if (a.startsWith("uscensus")) {
    // //     return -1;
    // //   } else if (b.startsWith("uscensus")) {
    // //     return 1;
    // //   } else if (a.startsWith("ipcc") && !b.startsWith("uscensus")) {
    // //     return -1;
    // //   }
    // //   return a.compareTo(b);
    // // });
    LOGGER.info("****** importGroupVersions = " + importGroupVersions);
    for (var importGroupVersion : importGroupVersions) {
      LOGGER.info("******* processing importGroupVersion = " + importGroupVersion);

      buildImportGroupPipeline(pipeline, null, importGroupVersion, cacheReader, spannerClient);
    }
  }

  static class ImportGroupKVTransform extends PTransform<PCollection<String>, PCollection<Void>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final SkipProcessing skipProcessing;

    public ImportGroupKVTransform(
        CacheReader cacheReader, SpannerClient spannerClient, SkipProcessing skipProcessing) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> cacheRows) {
      TupleTag<KV<String, Mutation>> observations = new TupleTag<>() {};
      TupleTag<KV<String, Mutation>> graph = new TupleTag<>() {};
      CacheRowKVMutationsDoFn createKVMutations =
          new CacheRowKVMutationsDoFn(
              cacheReader, spannerClient, skipProcessing, graph, observations);

      PCollectionTuple mutations =
          cacheRows.apply(
              "CreateKVMutations",
              ParDo.of(createKVMutations).withOutputTags(graph, TupleTagList.of(observations)));

      // Write graph to spanner.
      var graphResult =
          mutations
              .get(graph)
              .apply("Graph GBK", GroupByKey.create())
              .apply("ExtractGraphMutations", ParDo.of(new ExtractMutationsDoFn(spannerClient)))
              .apply("WriteGraphToSpanner", spannerClient.getWriteTransform());

      // Write observations to spanner.
      var obsResult =
          mutations
              .get(observations)
              .apply("Observation GBK", GroupByKey.create())
              .apply("ExtractObsMutations", ParDo.of(new ExtractObsMutationsDoFn()))
              .apply("WriteObsToSpanner", spannerClient.getWriteTransform());

      var writeResults = PCollectionList.of(graphResult.getOutput()).and(obsResult.getOutput());

      return writeResults.apply("Done", Flatten.pCollections());
    }
  }

  static class ExtractObsMutationsDoFn extends DoFn<KV<String, Iterable<Mutation>>, Mutation> {
    // private final SpannerClient spannerClient;
    private final Set<String> seenObs = new HashSet<>();
    private final AtomicInteger counter = new AtomicInteger();

    public ExtractObsMutationsDoFn() {}

    @StartBundle
    public void startBundle(StartBundleContext context) {
      this.seenObs.clear();
      this.counter.set(0);
    }

    @FinishBundle
    public void finishBundle() {
      LOGGER.info("Finish bundle: Total obs ={}, Unique ={}", counter.get(), seenObs.size());
      this.seenObs.clear();
      this.counter.set(0);
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Mutation>> kvs, OutputReceiver<Mutation> out) {
      var duplicateCount = 0;
      // sort the values by key
      var sortedMutations =
          StreamSupport.stream(kvs.getValue().spliterator(), false)
              .sorted(Comparator.comparing(this::getKey))
              .toList();
      counter.addAndGet(sortedMutations.size());
      // var set = new HashSet<>();
      for (Mutation mutation : sortedMutations) {
        if (seenObs.add(getKey(mutation))) {
          out.output(mutation);
        } else {
          duplicateCount++;
        }
      }
      if (duplicateCount > 0) {
        // LOGGER.info("Key=[{}]:duplicate mutations count=[{}]", kvs.getKey(), duplicateCount);
      }
    }

    private String getKey(Mutation m) {
      var mutationMap = m.asMap();
      String variableMeasured = mutationMap.get("variable_measured").getString();
      String observationAbout = mutationMap.get("observation_about").getString();
      var importName = mutationMap.get("import_name").getString();
      var observationPeriod = mutationMap.get("observation_period").getString();
      var measurementMethod = mutationMap.get("measurement_method").getString();
      var unit = mutationMap.get("unit").getString();
      var scalingFactor = mutationMap.get("scaling_factor").getString();

      return Joiner.on("::")
          .join(
              variableMeasured,
              observationAbout,
              importName,
              observationPeriod,
              measurementMethod,
              unit,
              scalingFactor);
    }
  }
}
