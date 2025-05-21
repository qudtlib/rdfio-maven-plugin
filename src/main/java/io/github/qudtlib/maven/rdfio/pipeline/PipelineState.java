package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

public class PipelineState {
    private Dataset dataset = DatasetFactory.create();
    private SavepointCache savepointCache;
    private boolean allowLoadingFromSavepoint = true;
    private String metadataGraph;
    private String shaclFunctionsGraph;
    private File baseDir;
    private List<Step> precedingSteps = new ArrayList<>();
    private String previousStepHash = "";
    private String pipelineId;
    private File outputBaseDir;
    private Log log;

    public PipelineState(
            String pipelineId,
            File baseDir,
            File outputBaseDir,
            Log log,
            String metadataGraph,
            String shaclFunctionsGraph) {
        Objects.requireNonNull(pipelineId, "Cannot create PipelineState: pipelineId is null");
        Objects.requireNonNull(baseDir, "Cannot create PipelineState: baseDir is null");
        Objects.requireNonNull(outputBaseDir, "Cannot create PipelineState: outputBaseDir is null");
        this.baseDir = baseDir;
        this.outputBaseDir = outputBaseDir;
        this.pipelineId = pipelineId;
        this.savepointCache = new SavepointCache(outputBaseDir, pipelineId);
        this.log = Optional.ofNullable(log).orElse(new StdoutLog());
        this.metadataGraph =
                Optional.ofNullable(metadataGraph).orElse(RDFIO.metadataGraph.toString());
        this.shaclFunctionsGraph =
                Optional.ofNullable(shaclFunctionsGraph)
                        .orElse(RDFIO.shaclFunctionsGraph.toString());
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

    public String getShaclFunctionsGraph() {
        return shaclFunctionsGraph;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public File getOutputBaseDir() {
        return outputBaseDir;
    }

    public Log getLog() {
        return log;
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
