package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.maven.plugins.annotations.Parameter;

public class Values {
    @Parameter private GraphSelection graphs;

    public GraphSelection getGraphs() {
        return graphs;
    }

    public void setGraphs(GraphSelection graphs) {
        this.graphs = graphs;
    }
}
