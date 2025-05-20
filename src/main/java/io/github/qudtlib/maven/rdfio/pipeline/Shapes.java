package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
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

    public void setGraphs(GraphSelection selection) {
        this.graphSelection = graphSelection;
    }

    public GraphSelection getGraphSelection() {
        return graphSelection;
    }

    // Shapes.java
    public static Shapes parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Shapes configuration is missing.
                            Usage: Provide a <shapes> element with either <file> or <graph>.
                            Example: <shapes><file>shapes.ttl</file></shapes>""");
        }

        Shapes shapes = new Shapes();
        Xpp3Dom fileDom = config.getChild("file");
        Xpp3Dom graphDom = config.getChild("graph");
        if (fileDom != null && fileDom.getValue() != null && !fileDom.getValue().trim().isEmpty()) {
            shapes.addFile(fileDom.getValue().trim());
        } else if (graphDom != null
                && graphDom.getValue() != null
                && !graphDom.getValue().trim().isEmpty()) {
            shapes.addGraph(graphDom.getValue().trim());
        } else {
            throw new MojoExecutionException(
                    """
                            Shapes requires one of <file> or <graph>.
                            Usage: Specify a SHACL shapes file or graph.
                            Examples:
                            - File: <file>shapes.ttl</file>
                            - Graph: <graph>shapes:graph</graph>""");
        }
        return shapes;
    }
}
