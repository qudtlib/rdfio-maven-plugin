package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.filter.GraphsHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import io.github.qudtlib.maven.rdfio.pipeline.support.PipelineConfigurationExeception;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

public class PipelineHelper {

    public static List<String> getGraphs(Dataset dataset, GraphSelection graphSelection) {
        if (graphSelection == null) return new ArrayList<>();
        List<String> result = new ArrayList<>();
        List<Pattern> includePatterns = new ArrayList<>();
        List<Pattern> excludePatterns = new ArrayList<>();

        // Convert include patterns to regex
        for (String include : graphSelection.getInclude()) {
            includePatterns.add(Pattern.compile(wildcardToRegex(include)));
        }

        // Convert exclude patterns to regex
        for (String exclude : graphSelection.getExclude()) {
            excludePatterns.add(Pattern.compile(wildcardToRegex(exclude)));
        }

        // Match graph names against patterns
        dataset.listModelNames()
                .forEachRemaining(
                        name -> {
                            for (Pattern includePattern : includePatterns) {
                                if (includePattern.matcher(name.toString()).matches()) {
                                    boolean excluded = false;
                                    for (Pattern excludePattern : excludePatterns) {
                                        if (excludePattern.matcher(name.toString()).matches()) {
                                            excluded = true;
                                            break;
                                        }
                                    }
                                    if (!excluded) {
                                        result.add(name.toString());
                                    }
                                    break; // Stop checking other include patterns once matched
                                }
                            }
                        });

        return result;
    }

    private static String wildcardToRegex(String wildcard) {
        StringBuilder regex = new StringBuilder();
        regex.append("^"); // Start of string
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    regex.append(".*"); // Match any characters
                    break;
                case '?':
                    regex.append("."); // Match any single character
                    break;
                default:
                    regex.append(Pattern.quote(String.valueOf(c))); // Escape special characters
                    break;
            }
        }
        regex.append("$"); // End of string
        return regex.toString();
    }

    /**
     * Prints the dataset to a String in pretty-printed TRIG format.
     *
     * @param dataset The Jena Dataset to serialize.
     * @return A String containing the dataset in TRIG format.
     * @throws RuntimeException if serialization fails.
     */
    public static String datasetToPrettyTrig(Dataset dataset) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            RDFDataMgr.write(out, dataset, RDFFormat.TRIG_PRETTY);
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize dataset to TRIG", e);
        }
    }

    public static void clearDataset(Dataset dataset) {
        dataset.getDefaultModel().removeAll();
        dataset.listModelNames()
                .forEachRemaining(graph -> dataset.getNamedModel(graph).removeAll());
        for (Iterator<String> it = dataset.listNames(); it.hasNext(); it.next()) {
            it.remove();
        }
    }

    /**
     * Prints the model to a String in pretty-printed TTL format.
     *
     * @param model The Jena Model to serialize.
     * @return A String containing the model in TTL format.
     * @throws RuntimeException if serialization fails.
     */
    public static String modelToPrettyTtl(Model model) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            RDFDataMgr.write(out, model, RDFFormat.TURTLE_PRETTY);
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize model to TRIG", e);
        }
    }

    public static void ensureGraphsExist(Dataset dataset, List<String> graphs, String kind) {
        for (String graph : graphs) {
            if (!dataset.containsNamedModel(graph)) {
                throw new PipelineConfigurationExeception(
                        "Configured %s graph %s does not exist in dataset".formatted(kind, graph));
            }
        }
    }

    public static String serializeMessageDigest(MessageDigest digest) {
        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static void bindGraphToFileIfUnbound(
            Dataset dataset, PipelineState state, RelativePath inputPath, String targetGraph) {
        Resource fileRes = inputPath.getRelativePathAsResource();
        if (isGraphUnbound(dataset, state, targetGraph)) {
            state.getLog()
                    .debug(
                            """
                           Binding
                                graph: %s
                              to file: %s
                                   so we can write the graph's content back to the file (graph %2$s has been previously unbound)"""
                                    .formatted(targetGraph, fileRes));
            Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
            metaModel.add(fileRes, RDFIO.loadsInto, ResourceFactory.createResource(targetGraph));
        }
    }

    public static void bindGraphToNoFileIfUnbound(
            Dataset dataset, PipelineState state, String targetGraph) {
        if (isGraphUnbound(dataset, state, targetGraph)) {
            state.getLog()
                    .debug(
                            """
                             Binding
                                graph: %s
                              to file: rdfio:NoFile (ie. the marker dummy 'file')
                              to prevent it from being bound to a file later on
                             """
                                    .formatted(targetGraph));
            Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
            metaModel.add(
                    RDFIO.NoFile, RDFIO.loadsInto, ResourceFactory.createResource(targetGraph));
        }
    }

    public static boolean isFileBoundToGraph(RelativePath inputPath, Model metaModel) {
        return metaModel.contains(
                inputPath.getRelativePathAsResource(), RDFIO.loadsInto, (RDFNode) null);
    }

    /**
     * Returns true if the there is a triple in the metadata graph: [file] rdfio:loadsInto [graph].
     * and [file] is not rdfio:NoFile .
     *
     * @param dataset
     * @param state
     * @param graphName
     * @return
     */
    public static boolean isGraphUnbound(Dataset dataset, PipelineState state, String graphName) {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        return !metaModel.contains(
                null, RDFIO.loadsInto, ResourceFactory.createResource(graphName));
    }

    public static void addGraphToGraph(
            Dataset dataset, String sourceGraph, String targetGraph, PipelineState state) {
        Model sourceModel = dataset.getNamedModel(sourceGraph);
        Model targetModel = dataset.getNamedModel(targetGraph);
        state.getLog()
                .debug(
                        """
                Copying
                    graph: %s
                 to graph: %s"""
                                .formatted(sourceGraph, targetGraph));
        bindGraphToNoFileIfUnbound(dataset, state, targetGraph);
        if (sourceModel != null) {
            targetModel.add(sourceModel);
        }
    }

    public static void addGraphsToDefaultGraph(
            Dataset dataset, List<String> inputGraphs, PipelineState state) {
        Model targetModel = dataset.getDefaultModel();
        for (String sourceGraph : inputGraphs) {
            Model sourceModel = dataset.getNamedModel(sourceGraph);
            if (sourceModel != null) {
                targetModel.add(sourceModel);
            }
        }
    }

    public static void addGraphsToGraph(
            Dataset dataset,
            String toGraphResolved,
            List<String> inputGraphs,
            PipelineState state) {
        Model targetModel = dataset.getNamedModel(toGraphResolved);
        for (String sourceGraph : inputGraphs) {
            Model sourceModel = dataset.getNamedModel(sourceGraph);
            if (sourceModel != null) {
                targetModel.add(sourceModel);
            }
        }
        bindGraphToNoFileIfUnbound(dataset, state, toGraphResolved);
    }

    public static void addDefaultModelToGraph(
            Dataset dataset, PipelineState state, String toGraph) {
        state.getLog()
                .debug(
                        """
                   Copying
                     graph: (default graph)
                  to graph: %s"""
                                .formatted(toGraph));
        dataset.getNamedModel(toGraph).add(dataset.getDefaultModel());
        bindGraphToNoFileIfUnbound(dataset, state, toGraph);
    }

    public static void readFileToGraph(
            Dataset dataset,
            PipelineState state,
            RelativePath inputPath,
            String targetGraph,
            boolean associateGraphWithFile) {
        Model model;
        if (targetGraph == null) {
            model = dataset.getDefaultModel();
        } else {
            model = dataset.getNamedModel(targetGraph);
        }
        List<File> files = List.of(inputPath.resolve());
        FileHelper.ensureFilesExist(files, "input");
        state.getLog()
                .debug(
                        """
         Loading
                file: %s
          into graph: %s"""
                                .formatted(inputPath.getRelativePath(), targetGraph));
        FileAccess.readRdf(inputPath, model, state);
        if (targetGraph != null) {
            if (associateGraphWithFile) {
                bindGraphToFileIfUnbound(dataset, state, inputPath, targetGraph);
            } else {
                bindGraphToNoFileIfUnbound(dataset, state, targetGraph);
            }
        }
    }

    public static void setPipelineVariable(
            Dataset dataset, PipelineState state, String variableName, RDFNode value) {
        state.getLog()
                .debug(
                        """
                          Setting pipeline variable
                            variablName: %s
                              new value: %s"""
                                .formatted(variableName, value));
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        Resource varRes = metaModel.createResource(RDFIO.VARIABLE_PREFIX + variableName);
        Property valueProp = RDFIO.value;
        metaModel.removeAll(varRes, valueProp, null);
        metaModel.add(varRes, valueProp, value);
    }

    public static List<String> getGraphList(Dataset dataset) {
        List<String> graphs = new ArrayList<>();
        Iterator<String> it = dataset.listNames();
        while (it.hasNext()) {
            graphs.add(it.next());
        }
        return graphs;
    }

    /**
     * Prints a concise summary of the pipeline state and dataset for debugging.
     *
     * @param state the PipelineState containing pipeline execution details
     */
    public static String formatPipelineStateSummary(Dataset dataset, PipelineState state) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PrintStream p = new PrintStream(out)) {

            p.println("=== Pipeline State Summary ===");

            // PipelineState details
            p.println("Pipeline ID: " + state.getPipelineId());
            p.println("Base Directory: " + state.getBaseDir().getAbsolutePath());
            p.println("Work Directory: " + state.getPipelineWorkDir().getRelativePath());
            p.println("Metadata Graph: " + state.getMetadataGraph());
            p.println("SHACL Functions Graph: " + state.getShaclFunctionsGraph());
            p.println(
                    "Preceding Steps: "
                            + state.getPrecedingSteps().stream()
                                    .map(step -> step.getElementName())
                                    .collect(Collectors.joining(", ")));
            p.println("Previous Step Hash: " + state.getPreviousStepHash());
            p.println("Allow Loading from Savepoint: " + state.isAllowLoadingFromSavepoint());

            // Dataset summary
            if (dataset == null) {
                p.println("Dataset: None");
            } else {

                // Graph sizes
                p.println("\nGraph Sizes:");
                Map<String, Long> graphSizes = GraphsHelper.getGraphSizes(dataset);
                if (graphSizes.isEmpty()) {
                    p.println("  No graphs available");
                } else {
                    graphSizes.forEach((name, size) -> p.printf("  %s: %d triples%n", name, size));
                }

                // Variable bindings
                p.println("\nVariable Bindings (from metadata graph):");
                QuerySolutionMap bindings =
                        SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
                if (bindings.asMap().isEmpty()) {
                    p.println("  No variable bindings");
                } else {
                    bindings.asMap()
                            .forEach(
                                    (var, node) ->
                                            p.printf(
                                                    "  ?%s = %s%n",
                                                    var,
                                                    node.isLiteral()
                                                            ? node.asLiteral().getString()
                                                            : node.asResource().getURI()));
                }

                // File/graph bindings
                p.println("\nFile/Graph Bindings (from metadata graph):");
                Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
                StmtIterator bindingIt =
                        metaModel.listStatements(null, RDFIO.loadsInto, (RDFNode) null);
                if (!bindingIt.hasNext()) {
                    p.println("  No file/graph bindings");
                } else {
                    while (bindingIt.hasNext()) {
                        Statement stmt = bindingIt.next();
                        String subject =
                                stmt.getSubject().isURIResource()
                                        ? stmt.getSubject().getURI()
                                        : stmt.getSubject().toString();
                        String graph = stmt.getObject().asResource().getURI();
                        p.printf("  %s -> %s%n", subject, graph);
                    }
                }

                p.println("=============================");
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    public static List<String> getFilePathBoundToGraph(
            Dataset dataset, PipelineState state, String graph) {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        StmtIterator it =
                metaModel.listStatements(
                        null, RDFIO.loadsInto, ResourceFactory.createResource(graph));
        List<String> files = new ArrayList<>();
        while (it.hasNext()) {
            String graphPath = it.next().getSubject().toString();
            if (!graphPath.equals(RDFIO.NoFile.toString())) {
                files.add(graphPath);
            }
        }
        return files;
    }

    public static boolean isFileBoundToGraph(
            Dataset dataset, PipelineState state, String fileArg, String graphName) {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        Resource fileRes = state.files().make(fileArg).getRelativePathAsResource();
        boolean found =
                metaModel.contains(
                        fileRes, RDFIO.loadsInto, ResourceFactory.createResource(graphName));
        return found;
    }

    public static String formatDefaultGraph() {
        return "[default graph]";
    }

    public static List<String> formatPaths(List<RelativePath> paths) {
        if (paths != null && !paths.isEmpty()) {
            return paths.stream()
                    .map(RelativePath::getRelativePath)
                    .sorted()
                    .map(s -> String.format(" file: %s", s))
                    .toList();
        }
        return List.of();
    }

    public static List<String> formatGraphs(List<String> graphs) {
        if (graphs != null && !graphs.isEmpty()) {
            return graphs.stream().sorted().map(s -> String.format("graph: %s", s)).toList();
        }
        return List.of();
    }
}
