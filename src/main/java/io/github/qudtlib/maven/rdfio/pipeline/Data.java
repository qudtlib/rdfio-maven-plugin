package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
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
    public static Data parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Data configuration is missing.
                            Usage: Provide a <data> element with optional <file>, <files>, <graph> or <graphs>.
                            Example: <data><file>data.ttl</file></data>""");
        }

        Data data = new Data();
        String file = ParsingHelper.getNonBlankChildString(config, "file");
        data.addFile(file);
        String graph = ParsingHelper.getNonBlankChildString(config, "graph");
        data.addGraph(graph);
        Xpp3Dom fileSelectionDom = config.getChild("files");
        if (fileSelectionDom != null) {
            data.setFileSelection(FileSelection.parse(fileSelectionDom));
        }
        Xpp3Dom graphSelectionDom = config.getChild("graphs");
        if (graphSelectionDom != null) {
            data.setGraphSelection(GraphSelection.parse(graphSelectionDom));
        }
        if (data.files.isEmpty()
                && data.fileSelection == null
                && data.graphs.isEmpty()
                && data.graphSelection == null) {
            throw new MojoExecutionException(
                    """
                            Data must have at least one nested element.
                            Usage: Provide a <data> element with optional <file>, <files>, <graph> or <graphs>.
                            Example: <data><file>data.ttl</file></data>""");
        }
        return data;
    }
}
