package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ClearStep implements Step {

    private String message = null;
    private final List<String> graphs = new ArrayList<>();
    private GraphSelection graphSelection;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

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

    @Override
    public String getElementName() {
        return "clear";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state)
            throws RuntimeException, MojoExecutionException {
        List<String> graphs =
                PipelineHelper.collectAllGraphReferences(
                        dataset, state, this.graphs, this.graphSelection);
        if (graphs.isEmpty()) {
            state.log().info("Clearing dataset");
        } else if (graphs.size() == 1) {
            state.log().info("Clearing graph:");
        } else {
            state.log().info("Clearing graphs:");
        }
        graphs.forEach(g -> state.log().info(g, 1));

        if (graphs.isEmpty()) {
            dataset.getDefaultModel().removeAll();
            Iterator<String> nameIt = dataset.listNames();
            while (nameIt.hasNext()) {
                dataset.removeNamedModel(nameIt.next());
            }
        } else {
            graphs.forEach(dataset::removeNamedModel);
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("clear".getBytes(StandardCharsets.UTF_8));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static ClearStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }
        ClearStep step = new ClearStep();
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, WriteStep::usage);
        ParsingHelper.optionalStringChildren(config, "graph", step::addGraph, WriteStep::usage);
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, step::setGraphSelection, WriteStep::usage);
        return step;
    }

    public static String usage() {
        return """
                           Usage:

                           Deletes graphs from the datset, or the whole dataset if no graphs are selected. In that case, Nothing remains, no shaclFunctions, no variables, no metadata.
                           Example:

                           <clear/>   clears the whole dataset

                           <clear>
                                <graph>my:graph</clear>
                                <graphs>
                                    <include>my:graphs:*</include>
                                </graphs>
                           </clear>
                           """;
    }
}
