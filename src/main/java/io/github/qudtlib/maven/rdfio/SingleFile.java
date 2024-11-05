package io.github.qudtlib.maven.rdfio;

import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugins.annotations.Parameter;

public class SingleFile {
    @Parameter private IncludeExcludePatterns input;

    @Parameter private String outputFile;

    @Parameter(defaultValue = "false")
    private boolean skip;

    @Parameter private Filters filters;

    public IncludeExcludePatterns getInput() {
        return input;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public Filters getFilters() {
        return filters;
    }

    public boolean isSkip() {
        return skip;
    }

    @Override
    public String toString() {
        return "Check{" + "shapes='" + input + '\'' + '}';
    }

    public void filter(Model inputGraph) {
        if (this.filters != null) {
            this.filters.filter(inputGraph);
        }
    }
}
