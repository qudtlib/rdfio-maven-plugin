package io.github.qudtlib.maven.rdfio.filter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.stream.Streams;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

public class GraphsHelper {

    /**
     * Returns a list of models from the dataset as identifified by the specified list of graph
     * names. If graphs is null or empty, or contains only the value "DEFAULT" (Graphs.DEFAULT), the
     * default graph is returned.
     *
     * <p>If graphs contains only the String "*" (no quotes, Graphs.EACH), all graphs as well as the
     * default graph are returned.
     *
     * <p>if graphs contains the value "DATASET" (no quotes, Graphs.DATASET), an exception is thrown
     * because this value indicates that the dataset is to be processed as a dataset not each graph
     * separately
     *
     * @param dataset
     * @param graphs
     * @return
     */
    public static List<Model> getModels(Dataset dataset, List<String> graphs) {
        if (graphs == null || graphs.isEmpty()) {
            return getModels(dataset, List.of(Graphs.DEFAULT.getGraphName()));
        }
        if (graphs.size() == 1 && graphs.get(0).trim().equals(Graphs.EACH.getGraphName())) {
            getModels(
                    dataset,
                    Stream.concat(
                                    Streams.of(dataset.listModelNames()).map(Object::toString),
                                    Stream.of(Graphs.DEFAULT.getGraphName()))
                            .toList());
        }
        List<Model> models = new ArrayList<>();
        for (String graphName : graphs) {
            Model model = null;
            if (graphName.equals(Graphs.DEFAULT.getGraphName())) {
                model = dataset.getDefaultModel();
            } else {
                model = dataset.getNamedModel(graphName);
                if (model == null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "No named graph found in RDF dataset for name %s", graphName));
                }
            }
            if (model != null) {
                models.add(model);
            }
        }
        return models;
    }

    public static String normalizeGraphName(String graph) {
        if (graph == null) {
            return Graphs.DEFAULT.getGraphName();
        }
        if (graph.toUpperCase(Locale.ROOT).equals(Graphs.DEFAULT.getGraphName())) {
            return Graphs.DEFAULT.getGraphName();
        }
        return graph;
    }

    public static List<Model> getAllModels(Dataset dataset) {
        List<Model> ret = new ArrayList<>();
        Iterator<Resource> it = dataset.listModelNames();
        while (it.hasNext()) {
            String modelName = it.next().toString();
            ret.add(dataset.getNamedModel(modelName));
        }
        ret.add(dataset.getDefaultModel());
        return ret;
    }

    public static long size(Dataset dataset) {
        return getAllModels(dataset).stream().mapToLong(Model::size).sum();
    }

    public static Model getModel(Dataset dataset, String graph) {
        if (Graphs.EACH.getGraphName().equals(graph)) {
            throw new IllegalArgumentException(
                    "Cannot get single model for reserved model name '"
                            + Graphs.EACH.getGraphName()
                            + "'");
        }
        return getModels(dataset, List.of(normalizeGraphName(graph))).get(0);
    }

    public static void retainSelected(Dataset dataset, List<String> graphs) {
        if (!graphs.isEmpty() && graphs.get(0).equals(Graphs.EACH.getGraphName())) {
            // retain everything
            return;
        }
        Set<String> normalizedGraphNames =
                graphs.stream().map(GraphsHelper::normalizeGraphName).collect(Collectors.toSet());
        for (String graphName : normalizedGraphNames) {
            if (Graphs.DEFAULT.getGraphName().equals(graphName)
                    || Graphs.EACH.getGraphName().equals(graphName)) {
                continue;
            }
            if (!dataset.containsNamedModel(graphName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Selected named graph %s does not exist in RDF dataset",
                                graphName));
            }
        }
        Iterator<Resource> it = dataset.listModelNames();
        List<String> graphsToRemove = new ArrayList<>();
        while (it.hasNext()) {
            String graphName = it.next().toString();
            if (!normalizedGraphNames.contains(graphName)) {
                graphsToRemove.add(graphName);
            }
        }
        for (String graphToRemove : graphsToRemove) {
            dataset.removeNamedModel(graphToRemove);
        }
        if (!normalizedGraphNames.contains(Graphs.DEFAULT.getGraphName())) {
            dataset.getDefaultModel().removeAll();
        }
    }

    public static Model unionAll(Dataset dataset) {
        Model union = dataset.getDefaultModel();
        Iterator<Resource> it = dataset.listModelNames();
        while (it.hasNext()) {
            String graphName = it.next().toString();
            union.add(dataset.getNamedModel(graphName));
        }
        return union;
    }

    public static boolean hasNamedGraphs(Dataset dataset) {
        return dataset.listModelNames().hasNext();
    }

    public static boolean hasNamedGraph(Dataset dataset, String graph) {
        String normalizedGraphName = normalizeGraphName(graph);
        if (Graphs.DEFAULT.getGraphName().equals(normalizedGraphName)) {
            return true;
        }
        return dataset.containsNamedModel(normalizedGraphName);
    }

    public static boolean isOnlyDefaultGraph(Collection<String> graphs) {
        if (graphs.isEmpty()
                || graphs.stream()
                        .allMatch(
                                g -> normalizeGraphName(g).equals(Graphs.DEFAULT.getGraphName()))) {
            return true;
        }
        return false;
    }

    public static Map<String, Long> getGraphSizes(Dataset dataset) {
        Map<String, Long> ret = new HashMap<>();
        ret.put(Graphs.DEFAULT.getGraphName(), dataset.getDefaultModel().size());
        Iterator it = dataset.listModelNames();
        while (it.hasNext()) {
            String modelName = it.next().toString();
            ret.put(modelName, dataset.getNamedModel(modelName).size());
        }
        return ret;
    }
}
