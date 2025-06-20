package io.github.qudtlib.maven.rdfio.filter;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class AbstractMultiGraphFilter extends AbstractFilter implements MultiGraphFilter {
    private List<String> graphs = new ArrayList<>();

    @Parameter
    public void setGraph(String graph) {
        this.getGraphs().add(graph);
    }

    @Override
    public List<String> getGraphs() {
        return graphs;
    }
}
