package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;

public interface Step {
    void execute(Dataset dataset, PipelineState state) throws MojoExecutionException;

    /**
     * Calculates a hash for the step based on its static inputs and configuration, incorporating
     * the previous step's hash.
     *
     * @param previousHash The hash of the previous step in the pipeline.
     * @return A string representing the step's hash.
     */
    String calculateHash(String previousHash);
}
