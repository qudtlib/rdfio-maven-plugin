package io.github.qudtlib.maven.rdfio.pipeline.step;

import static io.github.qudtlib.maven.rdfio.common.datasetchange.DatasetState.DEFAULT_GRAPH_NAME;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class WriteStep implements Step {
    private String message = null;
    private final List<String> graphs = new ArrayList<>();

    private String toFile;

    private GraphSelection graphSelection;

    public List<String> getGraphs() {
        return graphs;
    }

    public void addGraph(String graph) {
        this.graphs.add(graph);
    }

    public GraphSelection getGraphSelection() {
        return graphSelection;
    }

    public void setGraphSelection(GraphSelection graphSelection) {
        this.graphSelection = graphSelection;
    }

    public String getToFile() {
        return toFile;
    }

    public void setToFile(String toFile) {
        this.toFile = toFile;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String getElementName() {
        return "write";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (message != null) {
            state.log().info(state.variables().resolve(message, dataset));
        }
        // possible cases
        // 1. no toFile - each graph must have an associated file
        // 2. toFile is a triples format (eg. xyz.ttl) - the union of all graphs is written to the
        // file
        // 3. toFile is a quads format (eg.  xyz.trig) - each graph is written to the file with the
        // graph uri as the fourth element
        List<String> allGraphs = new ArrayList<>();
        if (this.graphs != null) {
            allGraphs.addAll(this.graphs);
        }
        if (this.graphSelection != null) {
            List<String> selectedGraphs = PipelineHelper.getGraphs(dataset, graphSelection, state);
            if (!selectedGraphs.isEmpty()) {
                allGraphs.addAll(selectedGraphs);
            }
        }
        if (this.toFile == null) {
            if (allGraphs.isEmpty()) {
                throw new MojoExecutionException(
                        "None of the specified <graph> or <graphs> found in dataset.%s"
                                .formatted(usage()));
            }
            writeOneFilePerGraph(dataset, state, allGraphs);
        } else {
            RelativePath outputPath =
                    state.files().make(state.variables().resolve(this.toFile, dataset));
            Lang outputLang = RDFLanguages.resourceNameToLang(outputPath.getName(), Lang.TTL);
            List<String> graphNames = null;
            if (RDFLanguages.isQuads(outputLang)) {
                state.files().createParentFolder(outputPath);
                Dataset dsToWrite = DatasetFactory.create();
                if (allGraphs.isEmpty()) {
                    dsToWrite.getDefaultModel().add(dataset.getDefaultModel());
                    graphNames = List.of(DEFAULT_GRAPH_NAME);
                } else {
                    graphNames = new ArrayList<>();
                    for (String graph : state.variables().resolve(allGraphs, dataset)) {
                        dsToWrite.addNamedModel(graph, dataset.getNamedModel(graph));
                        graphNames.add(graph);
                    }
                }
                state.files().writeRdf(outputPath, dsToWrite);
            } else {
                state.files().createParentFolder(outputPath);
                Model modelToWrite = ModelFactory.createDefaultModel();
                if (allGraphs.isEmpty()) {
                    modelToWrite.add(dataset.getDefaultModel());
                    graphNames = List.of(DEFAULT_GRAPH_NAME);
                } else {
                    graphNames = new ArrayList<>();
                    for (String graph : state.variables().resolve(allGraphs, dataset)) {
                        modelToWrite.add(dataset.getNamedModel(graph));
                        graphNames.add(graph);
                    }
                }
                state.files().writeRdf(outputPath, modelToWrite);
            }
            state.log().info(graphNames.stream().map(name -> "graph: " + name).toList(), 1);
            state.log().info("Output:", 1);
            state.log().info("file: " + outputPath.getRelativePath(), 2);
        }

        state.getPrecedingSteps().add(this);
    }

    private void writeOneFilePerGraph(
            Dataset dataset, PipelineState state, List<String> graphsToWrite)
            throws MojoExecutionException {
        if (graphsToWrite.isEmpty()) {
            throw new MojoExecutionException("No graphs found in dataset\n%s".formatted(usage()));
        }
        String outputFileStr;
        for (String graph : state.variables().resolve(graphsToWrite, dataset)) {
            List<String> files = PipelineHelper.getFilePathBoundToGraph(dataset, state, graph);
            if (files.size() == 1) {
                outputFileStr = files.get(0);
            } else if (files.isEmpty()) {
                throw new MojoExecutionException("No file mapping found for graph " + graph);
            } else {
                throw new MojoExecutionException("Multiple file mappings found for graph " + graph);
            }
            state.log()
                    .info(
                            List.of(
                                    "graph %s".formatted(graph),
                                    "    -> %s".formatted(outputFileStr)),
                            1);
            RelativePath outputPath = state.files().make(outputFileStr);
            state.files().createParentFolder(outputPath);
            state.files().writeRdf(outputPath, dataset.getNamedModel(graph));
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("write".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            if (graphSelection != null) {
                graphSelection.updateHash(digest, state);
            }
            graphs.forEach(graph -> digest.update(graph.getBytes(StandardCharsets.UTF_8)));

            if (toFile != null) {
                digest.update(toFile.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    // WriteStep.java
    public static WriteStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }

        WriteStep step = new WriteStep();
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, WriteStep::usage);
        ParsingHelper.optionalStringChildren(config, "graph", step::addGraph, WriteStep::usage);
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, step::setGraphSelection, WriteStep::usage);
        ParsingHelper.optionalStringChild(config, "toFile", step::setToFile, WriteStep::usage);
        if (step.graphs.isEmpty()
                && (step.graphSelection == null || step.graphSelection.getInclude().isEmpty())) {
            if (step.toFile == null) {
                throw new ConfigurationParseException(
                        config,
                        """
                            No <graph> or <graphs> specified, data is taken from the default graph.
                            In this case <toFile> must be specified
                            %s"""
                                .formatted(usage()));
            }
        }
        return step;
    }

    public static String usage() {
        return """
                           Usage:

                            Provide a <write> element with
                            - optional <graph> elements, listing graphs to write
                            - an optional <graphs> element with <include> and <exclude> subelements, which
                              take ant-style patterns
                            - and one optional <toFile> elements.

                            If <graph> and <graphs> is omitted, <toFile> must be present - in this case the content of the default
                            graph is written to the file.

                            The <toFile> element can be omitted if the system knows which file each graph
                            was read from, in which case it will overwrite that file with the contents of the graph.

                            The toFile element can have a triples or quads file extension (eg '.ttl' or '.trig').
                            In the first case, the union of the specified graphs is written to the file, in the latter,
                            individual graphs are written to the file.

                           Example:
                           <write>
                               <graph>test:graph</graph>
                               <graph>test:graph2</graph>
                               <toFile>output.ttl</toFile>
                           </write>""";
    }
}
