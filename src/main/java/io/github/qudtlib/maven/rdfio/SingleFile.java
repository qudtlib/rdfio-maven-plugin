package io.github.qudtlib.maven.rdfio;

import org.apache.maven.plugins.annotations.Parameter;

public class SingleFile {
    @Parameter private IncludeExcludePatterns input;

    @Parameter private String outputFile;

    @Parameter(defaultValue = "false")
    private boolean skip;

    public IncludeExcludePatterns getInput() {
        return input;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public boolean isSkip() {
        return skip;
    }

    @Override
    public String toString() {
        return "Check{" + "shapes='" + input + '\'' + '}';
    }
}
