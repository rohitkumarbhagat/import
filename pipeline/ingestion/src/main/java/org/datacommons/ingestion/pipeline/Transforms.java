package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.joda.time.Instant;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {

  /**
   * DoFn to convert arc cache rows to Spanner mutation groups. This function creates combined
   * mutation groups for both nodes and edges. DO NOT use this function if you want to write nodes
   * and edges separately.
   */
  public static class ArcRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ArcRowToMutationGroupDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<MutationGroup> c) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        List<Mutation> mutations =
            spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        List<MutationGroup> mutationGroups = spannerClient.toGraphMutationGroups(mutations);
        mutationGroups.forEach(c::output);
      }
    }
  }

  /** DoFn to convert obs time series cache rows to Spanner mutation groups. */
  public static class ObsTimeSeriesRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ObsTimeSeriesRowToMutationGroupDoFn(
        CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<MutationGroup> c) {
      if (CacheReader.isObsTimeSeriesCacheRow(row)) {
        List<Observation> observations = cacheReader.parseTimeSeriesRow(row);
        List<Mutation> mutations =
            observations.stream().map(spannerClient::toObservationMutation).toList();
        List<MutationGroup> mutationGroups = spannerClient.toObservationMutationGroups(mutations);
        mutationGroups.forEach(c::output);
      }
    }
  }

  public static class ObsTimeSeriesRowToMutationDoFn extends DoFn<String, Mutation> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ObsTimeSeriesRowToMutationDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<Mutation> c) {
      if (CacheReader.isObsTimeSeriesCacheRow(row)) {
        List<Observation> observations = cacheReader.parseTimeSeriesRow(row);
        List<Mutation> mutations =
            observations.stream().map(spannerClient::toObservationMutation).toList();
        mutations.forEach(c::output);
      }
    }
  }

  public static class ObsTimeSeriesRowToMutationKVDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ObsTimeSeriesRowToMutationKVDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<KV<String, Mutation>> c) {
      if (CacheReader.isObsTimeSeriesCacheRow(row)) {
        List<Observation> observations = cacheReader.parseTimeSeriesRow(row);
        List<Mutation> mutations =
            observations.stream().map(spannerClient::toObservationMutation).toList();
        mutations.stream().map(this::toKV).forEach(c::output);
      }
    }

    private KV<String, Mutation> toKV(Mutation mutation) {
      // Map<String, List<Mutation>> mutationMap = new HashMap<>();
      // Set<String> nodeIds = new HashSet<>();
      var mutationMap = mutation.asMap();
      String variableMeasured = mutationMap.get("variable_measured").getString();
      String observationAbout = mutationMap.get("observation_about").getString();
      var provenance = mutationMap.get("provenance").getString();
      var observationPeriod = mutationMap.get("observation_period").getString();

      return KV.of(variableMeasured + "::" + observationAbout, mutation);
    }
  }

  public static class LazyArcRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private Set<String> nodeIds;
    private Map<String, List<Mutation>> mutationMap;
    private int mutationCount = 0;

    public LazyArcRowToMutationGroupDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.mutationMap = new HashMap<>();
      this.nodeIds = new HashSet<>();
      this.mutationCount = 0;
    }

    @DoFn.FinishBundle
    public synchronized void FinishBundle(FinishBundleContext c) throws Exception {
      emitMutations(c);
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<MutationGroup> c) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        List<Mutation> mutations =
            spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        mutationCount += spannerClient.filterAndAddMutations(mutations, nodeIds, mutationMap);
        // if threshold reached, emit mutation groups
        if (mutationCount >= 10000) {
          emitMutations(c);
        }
      }
    }

    private void emitMutations(OutputReceiver<MutationGroup> c) {
      List<MutationGroup> mutationGroups = spannerClient.toMutationGroups(this.mutationMap);
      mutationGroups.forEach(c::output);
      // clear map and count
      // do not clear nodeIds
      this.mutationMap.clear();
      this.mutationCount = 0;
    }

    private void emitMutations(FinishBundleContext c) {
      List<MutationGroup> mutationGroups = spannerClient.toMutationGroups(this.mutationMap);
      Instant timestamp = Instant.now();
      mutationGroups.forEach(mg -> c.output(mg, timestamp, GlobalWindow.INSTANCE));
      // clear map and count
      // do not clear nodeIds
      this.mutationMap.clear();
      this.mutationCount = 0;
    }
  }

  public static class ArcRowToMutationDoFn extends DoFn<String, Mutation> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private Set<String> nodeIds;

    public ArcRowToMutationDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.nodeIds = new HashSet<>();
    }

    @DoFn.FinishBundle
    public synchronized void FinishBundle(FinishBundleContext c) throws Exception {
      // clear map and count
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<Mutation> c) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        List<Mutation> mutations =
            spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient.filterMutations(mutations, nodeIds).forEach(c::output);
      }
    }
  }

  public static class ArcRowToMutationKVDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private Set<String> nodeIds = null;

    public ArcRowToMutationKVDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
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
    public void processElement(@Element String row, OutputReceiver<KV<String, Mutation>> c) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        List<Mutation> mutations =
            spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient.filterMutations(mutations, nodeIds).stream()
            .map(this::toKV)
            .forEach(c::output);
      }
    }

    private KV<String, Mutation> toKV(Mutation mutation) {
      // Map<String, List<Mutation>> mutationMap = new HashMap<>();
      // Set<String> nodeIds = new HashSet<>();

      String subjectId = mutation.asMap().get("subject_id").getString();
      // Use first 15 chars as key
      var key = subjectId.substring(0, Math.min(subjectId.length(), 15));
      // var key = subjectId;
      return KV.of(key, mutation);
    }
  }
}
