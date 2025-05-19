package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class Data {
    @Parameter private List<String> file = new ArrayList<>();

    @Parameter private FileSelection files;

    @Parameter private List<String> graph = new ArrayList<>();

    public List<String> getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file.add(file);
    }

    public FileSelection getFiles() {
        return files;
    }

    public void setFiles(FileSelection files) {
        this.files = files;
    }

    public List<String> getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph.add(graph);
    }
}
