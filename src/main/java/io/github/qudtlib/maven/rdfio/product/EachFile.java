package io.github.qudtlib.maven.rdfio.product;

import io.github.qudtlib.maven.rdfio.filter.Filters;
import io.github.qudtlib.maven.rdfio.filter.Input;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;

public class EachFile implements Product {
    private final List<Input> inputs = new ArrayList<>();

    @Parameter(required = false)
    private String outputDir;

    private final List<String> graphs = new ArrayList<>();

    @Parameter(defaultValue = "false")
    private boolean replaceInputFiles;

    @Parameter(defaultValue = "false")
    private boolean skip;

    @Parameter private Filters filters;

    private Log log;

    @Parameter
    public void setInput(Input input) {
        this.inputs.add(input);
    }

    public List<Input> getInputs() {
        return inputs;
    }

    public List<String> getGraphs() {
        return graphs;
    }

    @Parameter
    public void setGraph(String graph) {
        this.graphs.add(graph);
    }

    public String getOutputDir() {
        return outputDir;
    }

    public Filters getFilters() {
        return filters;
    }

    public boolean isSkip() {
        return skip;
    }

    public boolean isReplaceInputFiles() {
        return replaceInputFiles;
    }

    @Override
    public String toString() {
        return "EachFile{"
                + "inputs="
                + inputs
                + ", outputDir='"
                + outputDir
                + '\''
                + ", graphs="
                + graphs
                + ", replaceInputFiles="
                + replaceInputFiles
                + ", skip="
                + skip
                + ", filters="
                + filters
                + '}';
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
        return "processing multiple files";
    }
}
