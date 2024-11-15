package io.github.qudtlib.maven.rdfio.product;

import io.github.qudtlib.maven.rdfio.filter.Filters;
import io.github.qudtlib.maven.rdfio.filter.IncludeExcludePatterns;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;

public class SingleFile implements Product {
    @Parameter private IncludeExcludePatterns input;

    @Parameter private String outputFile;

    @Parameter(defaultValue = "false")
    private boolean skip;

    @Parameter private Filters filters;

    private Log log;

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

    public void process(Model inputGraph) throws MojoExecutionException {
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
