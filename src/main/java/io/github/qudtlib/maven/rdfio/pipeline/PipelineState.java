package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.maven.plugin.MojoExecutionException;

public class PipelineState {
    private Dataset dataset = DatasetFactory.create();
    private SavepointCache savepointCache;
    private boolean allowLoadingFromSavepoint = true;
    private String metadataGraph;
    private File baseDir;
    private List<Step> precedingSteps = new ArrayList<>();
    private String previousStepHash = "";
    private String pipelineId;
    private File outputBaseDir;

    public PipelineState(
            String pipelineId, String metadataGraph, File baseDir, File outputBaseDir) {
        Objects.requireNonNull(pipelineId, "Cannot create PipelineState: pipelineId is null");
        Objects.requireNonNull(metadataGraph, "Cannot create PipelineState: metadataGraph is null");
        Objects.requireNonNull(baseDir, "Cannot create PipelineState: baseDir is null");
        Objects.requireNonNull(outputBaseDir, "Cannot create PipelineState: outputBaseDir is null");
        this.metadataGraph = metadataGraph;
        this.baseDir = baseDir;
        this.outputBaseDir = outputBaseDir;
        this.pipelineId = pipelineId;
        this.savepointCache = new SavepointCache(outputBaseDir, pipelineId);
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public SavepointCache getSavepointCache() {
        return savepointCache;
    }

    public boolean isAllowLoadingFromSavepoint() {
        return allowLoadingFromSavepoint;
    }

    public void setAllowLoadingFromSavepoint(boolean allowLoadingFromSavepoint) {
        this.allowLoadingFromSavepoint = allowLoadingFromSavepoint;
    }

    public String getMetadataGraph() {
        return metadataGraph;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public File getOutputBaseDir() {
        return outputBaseDir;
    }

    public List<Step> getPrecedingSteps() {
        return precedingSteps;
    }

    public String getPreviousStepHash() {
        return previousStepHash;
    }

    public void setPreviousStepHash(String previousStepHash) {
        this.previousStepHash = previousStepHash;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void requireUnderConfiguredDirs(File outputFile) throws MojoExecutionException {
        if (!FileHelper.isUnderDirectory(baseDir, outputFile)
                && !FileHelper.isUnderDirectory(outputBaseDir, outputFile)) {
            throw new MojoExecutionException(
                    "Cannot write file %s as it is neither under baseDir %s nor under baseOutputDir %s"
                            .formatted(outputFile, baseDir, outputBaseDir));
        }
    }
}
