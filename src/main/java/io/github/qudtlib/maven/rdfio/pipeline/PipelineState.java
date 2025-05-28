package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.*;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.SavepointCache;
import io.github.qudtlib.maven.rdfio.pipeline.support.VariableResolver;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.logging.Log;

public class PipelineState {
    private SavepointCache savepointCache;
    private boolean allowLoadingFromSavepoint = true;
    private String metadataGraph;
    private String shaclFunctionsGraph;
    private File baseDir;
    private List<Step> precedingSteps = new ArrayList<>();
    private String previousStepHash = "";
    private String pipelineId;
    private RelativePath pipelineWorkDir;
    private Log log;
    private Files files;
    private PipelineState.Variables variables;
    private String defaultShaclLogSeverity = null;
    private String defaultShaclFailSeverity = null;

    public PipelineState(
            String pipelineId,
            File baseDir,
            RelativePath pipelinesWorkDir,
            Log log,
            String metadataGraph,
            String shaclFunctionsGraph) {
        Objects.requireNonNull(pipelineId, "Cannot create PipelineState: pipelineId is null");
        Objects.requireNonNull(baseDir, "Cannot create PipelineState: baseDir is null");
        Objects.requireNonNull(
                pipelinesWorkDir, "Cannot create PipelineState: outputBaseDir is null");
        this.baseDir = baseDir;
        this.pipelineWorkDir = pipelinesWorkDir.subDir(pipelineId);
        this.pipelineId = pipelineId;
        this.savepointCache = new SavepointCache(this.pipelineWorkDir.subDir("savepoints"));
        this.log = Optional.ofNullable(log).orElse(new StdoutLog());
        this.metadataGraph =
                Optional.ofNullable(metadataGraph).orElse(RDFIO.metadataGraph.toString());
        this.shaclFunctionsGraph =
                Optional.ofNullable(shaclFunctionsGraph)
                        .orElse(RDFIO.shaclFunctionsGraph.toString());
        this.files = new Files();
        this.variables = new PipelineState.Variables();
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

    public RelativePath getPipelineWorkDir() {
        return pipelineWorkDir;
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

    public String getDefaultShaclLogSeverity() {
        return defaultShaclLogSeverity;
    }

    public void setDefaultShaclLogSeverity(String defaultShaclLogSeverity) {
        this.defaultShaclLogSeverity = defaultShaclLogSeverity;
    }

    public void requireUnderBaseDir(File file) throws ForbiddenFilePathException {
        if (!FileHelper.isUnderDirectory(baseDir, file)) {
            throw new ForbiddenFilePathException(
                    "Cannot access file %s as it is not under baseDir %s"
                            .formatted(file, baseDir.getAbsolutePath()));
        }
    }

    public String getDefaultShaclFailSeverity() {
        return defaultShaclFailSeverity;
    }

    public void setDefaultShaclFailSeverity(String defaultShaclFailSeverity) {
        this.defaultShaclFailSeverity = defaultShaclFailSeverity;
    }

    public void requireUnderBaseDir(RelativePath file) throws ForbiddenFilePathException {
        if (!FileHelper.isUnderDirectory(baseDir, file.resolve())) {
            throw new ForbiddenFilePathException(
                    "Cannot access file %s as it is not under baseDir %s".formatted(file, baseDir));
        }
    }

    public Files files() {
        return this.files;
    }

    public class Files {

        public RelativePath make(String relativePath) {
            return new RelativePath(baseDir, relativePath);
        }

        public RelativePath makeRelativeToOutputBase(String relativeToOutputBase) {
            RelativePath rto = new RelativePath(pipelineWorkDir.resolve(), relativeToOutputBase);
            return rto.rebase(baseDir);
        }

        public List<RelativePath> make(List<String> relativePath) {
            return relativePath.stream().map(s -> new RelativePath(baseDir, s)).toList();
        }

        public void readRdf(RelativePath path, Model model) throws FileAccessException {
            FileAccess.readRdf(path, model, PipelineState.this);
        }

        public void readRdf(RelativePath path, Dataset dataset) throws FileAccessException {
            FileAccess.readRdf(path, dataset, PipelineState.this);
        }

        public void writeRdf(RelativePath path, Dataset dataset) throws FileAccessException {
            FileAccess.writeRdf(path, dataset, PipelineState.this);
        }

        public void writeRdf(RelativePath path, Model model) throws FileAccessException {
            FileAccess.writeRdf(path, model, PipelineState.this);
        }

        public String readText(RelativePath path) throws FileAccessException {
            return FileAccess.readText(path, PipelineState.this);
        }

        public void writeText(RelativePath path, String content) throws FileAccessException {
            FileAccess.writeText(path, content, PipelineState.this);
        }

        public void delete(RelativePath path) throws FileAccessException {
            FileAccess.delete(path, PipelineState.this);
        }

        public boolean exists(RelativePath path) {
            return FileAccess.exists(path, PipelineState.this);
        }

        public boolean createParentFolder(RelativePath path) {
            return FileAccess.createParentFolder(path, PipelineState.this);
        }

        public boolean mkdirs(RelativePath path) {
            return FileAccess.mkdirs(path, PipelineState.this);
        }
    }

    public Variables variables() {
        return this.variables;
    }

    public class Variables {
        public String resolve(String input, Dataset dataset) {
            return VariableResolver.resolveVariables(input, dataset, metadataGraph);
        }

        public List<String> resolve(List<String> input, Dataset dataset) {
            return VariableResolver.resolveVariables(input, dataset, metadataGraph);
        }

        public FileSelection resolve(FileSelection input, Dataset dataset) {
            return VariableResolver.resolveVariables(input, dataset, metadataGraph);
        }

        public GraphSelection resolve(GraphSelection input, Dataset dataset) {
            return VariableResolver.resolveVariables(input, dataset, metadataGraph);
        }
    }

    private VariableResolver variableResolver;
}
