package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.maven.plugins.annotations.Parameter;

public class Inferred {
    @Parameter private String graph;

    @Parameter private String file;

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }
}
