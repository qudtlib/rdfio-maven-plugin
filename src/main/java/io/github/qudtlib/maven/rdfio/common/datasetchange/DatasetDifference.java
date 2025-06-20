package io.github.qudtlib.maven.rdfio.common.datasetchange;

import static java.util.function.Predicate.not;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record DatasetDifference(Set<GraphDifference> differences) {
    public static DatasetDifference of(DatasetState left, DatasetState right) {
        Set<GraphDifference> graphDifferences = new HashSet<>();
        Set<String> namesFound = new HashSet<>();
        for (String leftGraphName : left.getGraphNames()) {
            if (namesFound.contains(leftGraphName)) {
                continue;
            }
            namesFound.add(leftGraphName);
            addGraphDifference(left, right, leftGraphName, graphDifferences);
        }
        for (String rightGraphName : right.getGraphNames()) {
            if (namesFound.contains(rightGraphName)) {
                continue;
            }
            namesFound.add(rightGraphName);
            addGraphDifference(left, right, rightGraphName, graphDifferences);
        }
        return new DatasetDifference(graphDifferences);
    }

    private static void addGraphDifference(
            DatasetState left,
            DatasetState right,
            String graphName,
            Set<GraphDifference> differences) {
        differences.add(
                new GraphDifference(
                        graphName,
                        left.containsGraphName(graphName),
                        left.getGraphSize(graphName),
                        right.containsGraphName(graphName),
                        right.getGraphSize(graphName),
                        Objects.equals(
                                left.getGraphHash(graphName), right.getGraphHash(graphName))));
    }

    public boolean isDifferent() {
        return !(differences.stream()
                .allMatch(d -> d.graphsAreEqual() && d.sizeLeft() == d.sizeRight()));
    }

    public List<String> formatForChange() {
        if (!isDifferent()) {
            return List.of("none");
        }
        return differences.stream()
                .filter(not(GraphDifference::graphsAreEqual))
                .map(GraphDifference::formatForChange)
                .sorted()
                .toList();
    }
}
