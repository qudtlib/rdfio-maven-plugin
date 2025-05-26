package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.jena.query.Dataset;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class InputsComponent<T extends Step> implements StepComponent<T> {

    T owner = null;

    private final List<String> files = new ArrayList<>();

    private FileSelection fileSelection;

    private final List<String> graphs = new ArrayList<>();

    private GraphSelection graphSelection;

    public InputsComponent(T owner) {
        this.owner = owner;
    }

    public static <S extends Step> Function<Xpp3Dom, InputsComponent<S>> getParseFunction(S owner) {
        return config -> InputsComponent.parse(owner, config);
    }

    public List<String> getFiles() {
        return files;
    }

    public void addFile(String file) {
        this.files.add(file);
    }

    public FileSelection getFileSelection() {
        return fileSelection;
    }

    public void setFileSelection(FileSelection fileSelection) {
        this.fileSelection = fileSelection;
    }

    public List<String> getGraphs() {
        return graphs;
    }

    public void addGraph(String graph) {
        this.graphs.add(graph);
    }

    public void setGraphSelection(GraphSelection graphSelection) {
        this.graphSelection = graphSelection;
    }

    public GraphSelection getGraphSelection() {
        return graphSelection;
    }

    public boolean hasNoInputs() {
        return this.files.isEmpty()
                && this.fileSelection == null
                && this.graphs.isEmpty()
                && this.graphSelection == null;
    }

    @Override
    public T getOwner() {
        return owner;
    }

    public static <T extends Step> InputsComponent<T> parse(T owner, Xpp3Dom config) {
        InputsComponent<T> inputsComponent = new InputsComponent<>(owner);
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            %1$s step input configuration is missing.
                            %s"""
                            .formatted(owner.getElementName(), inputsComponent.usage()));
        }

        ParsingHelper.optionalStringChildren(
                config, "file", inputsComponent::addFile, inputsComponent::usage);
        ParsingHelper.optionalStringChildren(
                config, "graph", inputsComponent::addGraph, inputsComponent::usage);
        ParsingHelper.optionalDomChild(
                config,
                "files",
                FileSelection::parse,
                inputsComponent::setFileSelection,
                inputsComponent::usage);
        ParsingHelper.optionalDomChild(
                config,
                "graphs",
                GraphSelection::parse,
                inputsComponent::setGraphSelection,
                inputsComponent::usage);
        return inputsComponent;
    }

    @Override
    public String usage() {
        return """
                Usage: Provide inputs via <file>, <files>, <graph> or <graphs> element
                                            Example:
                                            <%1$s>
                                                <file>data.ttl</file>
                                                <toGraph>test:graph</toGraph>
                                        </%1$s>"""
                .formatted(owner.getElementName());
    }

    public List<RelativePath> getAllInputPaths(Dataset dataset, PipelineState state) {
        List<RelativePath> result = new ArrayList<>();
        result.addAll(state.files().make(state.variables().resolve(getFiles(), dataset)));
        result.addAll(
                FileAccess.resolveFileSelection(
                        state.variables().resolve(getFileSelection(), dataset),
                        state.getBaseDir()));
        return result;
    }

    public List<String> getAllInputGraphs(Dataset dataset, PipelineState state) {
        List<String> allGraphs = new ArrayList<>();
        allGraphs.addAll(state.variables().resolve(this.graphs, dataset));
        allGraphs.addAll(
                PipelineHelper.getGraphs(
                        dataset, state.variables().resolve(graphSelection, dataset)));
        return allGraphs;
    }

    public void updateHash(MessageDigest digest, PipelineState state) {
        RdfFileProcessor.updateHashWithFiles(
                RdfFileProcessor.resolveFiles(files, fileSelection, state.getBaseDir()), digest);
        if (fileSelection != null) {
            for (String include : fileSelection.getInclude()) {
                digest.update(include.getBytes(StandardCharsets.UTF_8));
            }
            for (String exclude : fileSelection.getExclude()) {
                digest.update(exclude.getBytes(StandardCharsets.UTF_8));
            }
        }
        for (String g : graphs) {
            digest.update(g.getBytes(StandardCharsets.UTF_8));
        }
    }
}
