package org.datacommons.pipeline.differ;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.pipeline.util.GraphUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(DifferPipeline.class);
  private static final String DIFF_HEADER =
      "variableMeasured,observationAbout,observationDate,observationPeriod,measurementMethod,unit,scalingFactor,valueCurrent,valuePrevious,diff";

  public static void main(String[] args) throws Exception {

    // Create the pipeline.
    DifferOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DifferOptions.class);
    Pipeline p = Pipeline.create(options);

    // Read input graph files and convert into PCollections.
    PCollection<McfGraph> previous_nodes, current_nodes;
    if (options.getUseOptimizedGraphFormat()) {
      LOGGER.info("Using tfrecord file format");
      current_nodes = GraphUtils.readMcfGraph(options.getCurrentData(), p);
      previous_nodes = GraphUtils.readMcfGraph(options.getPreviousData(), p);
    } else {
      LOGGER.info("Using mcf file format");
      previous_nodes = GraphUtils.readMcfFile(options.getPreviousData(), p);
      current_nodes = GraphUtils.readMcfFile(options.getCurrentData(), p);
    }

    // Process the input and perform diff operation.
    PCollection<KV<String, String>> nCollection = DifferUtils.processGraph(current_nodes);
    PCollection<KV<String, String>> pCollection = DifferUtils.processGraph(previous_nodes);
    PCollection<String> merged = DifferUtils.performDiff(nCollection, pCollection);

    merged.apply(
        "Write diff output",
        TextIO.write().to(options.getOutputLocation()).withSuffix(".csv").withHeader(DIFF_HEADER));
    p.run();
  }
}
