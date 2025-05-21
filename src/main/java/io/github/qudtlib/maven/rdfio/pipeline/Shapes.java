package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Shapes {
    private final List<String> files = new ArrayList<>();

    private FileSelection fileSelection;

    private final List<String> graphs = new ArrayList<>();

    private GraphSelection graphSelection;

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

    public void setGraphSelection(GraphSelection selection) {
        this.graphSelection = selection;
    }

    public GraphSelection getGraphSelection() {
        return graphSelection;
    }

    // Shapes.java
    public static Shapes parse(Xpp3Dom config) {
        if (config == null || config.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    """
                            Shapes configuration is missing.
                            Usage: Provide a <shapes> element with either <file> or <graph>.
                            Example: <shapes><file>shapes.ttl</file></shapes>""");
        }

        Shapes shapes = new Shapes();
        ParsingHelper.optionalStringChildren(config, "file", shapes::addFile, Data::usage);
        ParsingHelper.optionalStringChildren(config, "graph", shapes::addGraph, Data::usage);
        ParsingHelper.optionalDomChild(
                config, "files", FileSelection::parse, shapes::setFileSelection, Data::usage);
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, shapes::setGraphSelection, Data::usage);
        if (shapes.files.isEmpty()
                && shapes.fileSelection == null
                && shapes.graphs.isEmpty()
                && shapes.graphSelection == null) {
            throw new ConfigurationParseException(
                    """
                            Shapes requires one of <file>, <files>, <graph> or <graphs>.
                            %s"""
                            .formatted(usage()));
        }
        return shapes;
    }

    public static String usage() {
        return """
                            Usage: Provide a <shapes> element with either <file> or <graph>.
                            Example: <shapes><file>shapes.ttl</file></shapes>""";
    }
}
