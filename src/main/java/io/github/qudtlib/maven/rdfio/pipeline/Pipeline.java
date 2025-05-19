package io.github.qudtlib.maven.rdfio.pipeline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class Pipeline {
    @Parameter(required = true)
    private String pipelineId;

    @Parameter(defaultValue = RDFIO.metadataGraphString)
    private String metadataGraph;

    @Parameter(defaultValue = "false")
    private boolean forceRun;

    @Parameter(defaultValue = "${project.basedir}")
    private File baseDir;

    @Parameter private List<Step> steps = new ArrayList<>();

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public String getMetadataGraph() {
        return metadataGraph;
    }

    public void setMetadataGraph(String metadataGraph) {
        this.metadataGraph = metadataGraph;
    }

    public boolean isForceRun() {
        return forceRun;
    }

    public void setForceRun(boolean forceRun) {
        this.forceRun = forceRun;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(File baseDir) {
        this.baseDir = baseDir;
    }

    public List<Step> getSteps() {
        return steps;
    }

    public void setSteps(List<Step> steps) {
        this.steps = steps;
    }
}
