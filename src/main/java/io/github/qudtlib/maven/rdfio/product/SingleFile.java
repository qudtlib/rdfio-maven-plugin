package io.github.qudtlib.maven.rdfio.product;

import io.github.qudtlib.maven.rdfio.filter.Filters;
import io.github.qudtlib.maven.rdfio.filter.Input;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;

public class SingleFile implements Product {

    private List<Input> inputs = new ArrayList<>();

    private List<String> graphs = new ArrayList<>();

    @Parameter private String outputFile;

    @Parameter(defaultValue = "false")
    private boolean skip;

    @Parameter private Filters filters;

    private Log log;

    @Parameter
    public void setGraph(String graph) {
        this.graphs.add(graph);
    }

    public List<Input> getInputs() {
        return inputs;
    }

    @Parameter
    public void setInput(Input input) {
        this.inputs.add(input);
    }

    public String getOutputFile() {
        return outputFile;
    }

    public Filters getFilters() {
        return filters;
    }

    public List<String> getGraphs() {
        return graphs;
    }

    public boolean isSkip() {
        return skip;
    }

    @Override
    public String toString() {
        return "Check{" + "shapes='" + inputs + '\'' + '}';
    }

    public void process(Dataset inputGraph) throws MojoExecutionException {
        if (this.filters != null) {
            this.filters.setLog(log);
            this.filters.filter(inputGraph);
        }
    }

    public void setLog(Log log) {
        this.log = log;
    }

    @Override
    public String describe() {
        return getOutputFile();
    }
}
