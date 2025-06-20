package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.TimeHelper;
import io.github.qudtlib.maven.rdfio.common.datasetchange.DatasetDifference;
import io.github.qudtlib.maven.rdfio.common.datasetchange.DatasetState;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;

public interface Step {

    /**
     * Wraps execute() and wraps any RuntimeException in a MojoExecutionException.
     *
     * @param dataset
     * @param state
     * @throws MojoExecutionException
     */
    default void executeAndWrapException(Dataset dataset, PipelineState state)
            throws MojoExecutionException {
        try {
            long start = System.currentTimeMillis();
            DatasetState stateBefore = new DatasetState(dataset);
            state.log().info("");
            state.log().info("Executing <%s> step ".formatted(getElementName()));
            execute(dataset, state);
            DatasetState stateAfter = new DatasetState(dataset);
            state.log().info("Dataset changes:", 1);
            state.log().info(DatasetDifference.of(stateBefore, stateAfter).formatForChange(), 2);
            state.log()
                    .info(
                            "duration: "
                                    + TimeHelper.makeDurationString(
                                            System.currentTimeMillis() - start),
                            1);
        } catch (RuntimeException e) {
            throw new MojoExecutionException(
                    "Error executing <%s> step: %s".formatted(getElementName(), e.getMessage()), e);
        }
    }

    String getElementName();

    void execute(Dataset dataset, PipelineState state)
            throws RuntimeException, MojoExecutionException;

    /**
     * Calculates a hash for the step based on its static inputs and configuration, incorporating
     * the previous step's hash.
     *
     * @param previousHash The hash of the previous step in the pipeline.
     * @param state
     * @return A string representing the step's hash.
     */
    String calculateHash(String previousHash, PipelineState state);
}
