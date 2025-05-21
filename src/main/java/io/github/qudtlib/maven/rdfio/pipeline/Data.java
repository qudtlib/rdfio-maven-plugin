package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Data {
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

    // Data.java
    public static Data parse(Xpp3Dom config) {
        if (config == null || config.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    """
                            Data configuration is missing.
                            %s"""
                            .formatted(usage()));
        }

        Data data = new Data();
        ParsingHelper.optionalStringChildren(config, "file", data::addFile, Data::usage);
        ParsingHelper.optionalStringChildren(config, "graph", data::addGraph, Data::usage);
        ParsingHelper.optionalDomChild(
                config, "files", FileSelection::parse, data::setFileSelection, Data::usage);
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, data::setGraphSelection, Data::usage);
        if (data.files.isEmpty()
                && data.fileSelection == null
                && data.graphs.isEmpty()
                && data.graphSelection == null) {
            throw new ConfigurationParseException(
                    "Data must have at least one nested element.\n" + usage());
        }
        return data;
    }

    public static String usage() {
        return """
                    Usage: Provide a <data> element with optional <file>, <files>, <graph> or <graphs>.
                    Example: <data><file>data.ttl</file></data>""";
    }
}
