package io.github.qudtlib.maven.rdfio.common.datasetchange;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.jena.query.Dataset;

public class DatasetState {
    private static final String DEFAULT_GRAPH_NAME = "[DEFAULT GRAPH]";
    private final Set<String> graphNames = new HashSet<>();
    private final Map<String, Long> graphSizes = new HashMap<>();
    private final Map<String, Integer> graphHashes = new HashMap<>();

    public DatasetState(Dataset dataset) {
        graphNames.addAll(PipelineHelper.getGraphList(dataset));
        for (String graphName : graphNames) {
            graphSizes.put(graphName, dataset.getNamedModel(graphName).size());
            graphHashes.put(
                    graphName,
                    dataset
                            .getNamedModel(graphName)
                            .listStatements()
                            .mapWith(s -> s.hashCode())
                            .toList()
                            .stream()
                            .mapToInt(x -> x)
                            .sum());
        }
        graphSizes.put(DEFAULT_GRAPH_NAME, dataset.getDefaultModel().size());
        graphHashes.put(
                DEFAULT_GRAPH_NAME,
                dataset
                        .getDefaultModel()
                        .listStatements()
                        .mapWith(s -> s.hashCode())
                        .toList()
                        .stream()
                        .mapToInt(x -> x)
                        .sum());
        graphNames.add(DEFAULT_GRAPH_NAME);
    }

    public Set<String> getGraphNames() {
        return graphNames;
    }

    public Map<String, Long> getGraphSizes() {
        return graphSizes;
    }

    public Map<String, Integer> getGraphHashes() {
        return graphHashes;
    }

    public long getGraphSize(String graphName) {
        Long size = this.graphSizes.get(graphName);
        if (size != null) {
            return size;
        }
        return 0;
    }

    public boolean containsGraphName(String graphName) {
        return this.graphNames.contains(graphName);
    }

    public Integer getGraphHash(String graphName) {
        return this.graphHashes.get(graphName);
    }
}
