package io.github.qudtlib.maven.rdfio.filter;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import org.apache.maven.plugins.annotations.Parameter;

public class Input extends FileSelection {
    private String graph;

    @Parameter
    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getGraph() {
        return graph;
    }

    @Override
    public String toString() {
        return "IncludeExcludePatterns{"
                + "graph='"
                + graph
                + '\''
                + ", include="
                + getInclude()
                + ", exclude="
                + getExclude()
                + '}';
    }
}
