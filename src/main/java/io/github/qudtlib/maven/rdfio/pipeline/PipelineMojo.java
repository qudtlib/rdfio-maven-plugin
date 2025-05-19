package io.github.qudtlib.maven.rdfio.pipeline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "pipeline")
public class PipelineMojo extends AbstractMojo {

    @Parameter private Pipeline pipeline;
    Dataset dataset = null;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            if (pipeline == null) {
                throw new MojoExecutionException("Pipeline configuration is required");
            }
            // Assign default savepoint IDs
            int savepointCount = 0;
            for (Step step : pipeline.getSteps()) {
                if (step instanceof SavepointStep) {
                    savepointCount++;
                    SavepointStep savepoint = (SavepointStep) step;
                    if (savepoint.getId() == null) {
                        savepoint.setId(String.format("sp%03d", savepointCount));
                    }
                }
            }
            dataset = DatasetFactory.create();
            PipelineState state =
                    new PipelineState(
                            pipeline.getPipelineId(),
                            pipeline.getMetadataGraph(),
                            pipeline.getBaseDir(),
                            new File("target"));
            state.setAllowLoadingFromSavepoint(!pipeline.isForceRun());

            int startIndex = -1;
            List<String> stepHashes = new ArrayList<>();

            if (!pipeline.isForceRun()) {
                // Compute hashes for all steps
                String previousHash = "";
                for (Step step : pipeline.getSteps()) {
                    String hash = step.calculateHash(previousHash);
                    stepHashes.add(hash);
                    previousHash = hash;
                }

                // Check savepoints in reverse order
                for (int i = pipeline.getSteps().size() - 1; i >= 0; i--) {
                    Step step = pipeline.getSteps().get(i);
                    if (step instanceof SavepointStep) {
                        SavepointStep savepoint = (SavepointStep) step;
                        if (savepoint.isValid(dataset, state, stepHashes.get(i))) {
                            startIndex = i; // Start with the valid savepoint
                            break;
                        }
                    }
                }
            }
            // Execute pipeline from startIndex
            String previousHash;
            if (startIndex == -1) {
                startIndex = 0; // No valid savepoint, start from beginning
            }
            if (startIndex == 0) {
                previousHash = "";
            } else {
                previousHash = stepHashes.get(startIndex - 1);
            }
            for (int i = startIndex; i < pipeline.getSteps().size(); i++) {
                Step step = pipeline.getSteps().get(i);
                state.setPreviousStepHash(previousHash);
                previousHash = step.calculateHash(previousHash);
                step.execute(dataset, state);
            }
        } catch (Throwable throwable) {
            throw new MojoExecutionException("Error executing PipelineMojo", throwable);
        }
    }

    /**
     * Package-private getter for testing purposes.
     *
     * @return
     */
    Dataset getDataset() {
        return dataset;
    }
}
