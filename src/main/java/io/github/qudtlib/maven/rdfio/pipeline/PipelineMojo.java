package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.step.SavepointStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.PluginExecution;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;

@Mojo(name = "pipeline")
public class PipelineMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject project;

    @Parameter(defaultValue = "${project.basedir}", readonly = true)
    private File baseDir;

    @Parameter(defaultValue = "${project.build.directory}", readonly = true)
    private File workBaseDir;

    @Parameter(defaultValue = "${mojoExecution}", readonly = true)
    private MojoExecution mojoExecution;

    /**
     * If <code>true</code>, the pipeline will disregard any valid savepoints and run from the
     * beginning.
     */
    @Parameter(property = "rdfio.pipeline.forceRun")
    private String forceRun;

    /**
     * If set to the id of a **valid** savepoint, pipeline execution will start at that savepoint,
     * instead of the latest valid one. Note: rdfio.pipeline.forceRun overrides this setting.
     */
    @Parameter(property = "rdfio.pipeline.resumeAtSavepointId")
    private String resumeAtSavepointId;

    private Xpp3Dom configuration;

    private Pipeline pipeline;

    Dataset dataset = null;

    private Xpp3Dom getExecutionConfiguration() throws MojoExecutionException {
        getLog().debug("searching pipeline plugin configuration");
        for (Plugin plugin : project.getBuildPlugins()) {
            if (plugin.getArtifactId().equals(mojoExecution.getArtifactId())) {
                PluginExecution execution =
                        plugin.getExecutionsAsMap().get(mojoExecution.getExecutionId());
                if (execution != null) {
                    return (Xpp3Dom) execution.getConfiguration();
                }
            }
        }
        throw new MojoExecutionException(
                "Unable to find configuration for plugin %s, goal %s, execution %s"
                        .formatted(
                                mojoExecution.getArtifactId(),
                                mojoExecution.getGoal(),
                                mojoExecution.getExecutionId()));
    }

    @Override
    public void execute() throws MojoExecutionException {
        try {
            if (pipeline == null) {
                parseConfiguration();
            }
            if (pipeline == null) {
                throw new MojoExecutionException("Pipeline configuration is required");
            }
            // Apply forceRun from Maven property
            // set it to anything else than 'false', including nothing, force is activated
            if (forceRun != null) {
                if (forceRun.toString().toLowerCase(Locale.ROOT).equals("false")) {
                    getLog().info(
                                    "Pipeline '%s' has been configured to start from the latest valid savepoint using maven property 'rdfio.pipeline.forceRun=false'"
                                            .formatted(pipeline.getId()));
                    pipeline.setForceRun(false);
                } else {
                    getLog().info(
                                    "Pipeline '%s' is has been forced to run from the start using maven property 'rdfio.pipeline.forceRun'"
                                            .formatted(pipeline.getId()));
                    pipeline.setForceRun(true);
                }
            } else {
                // resumeAtSavepointId overrides static pipeline forceRun Configuration
                if (resumeAtSavepointId == null) {
                    if (pipeline.isForceRun()) {
                        getLog().info(
                                        "Pipeline '%s' is configured to run from the start, ignoring any valid savepoints (to override, use '-Drdfio.pipeline.forceRun=false' or '-Drdfio.pipeline.resumeAtSavepointId=(pipelineId:)savepointId,...')"
                                                .formatted(pipeline.getId()));
                    } else {
                        getLog().info(
                                        "Pipeline '%s' is configured to start from the lastest valid savepoint (to override, use '-Drdfio.pipeline.forceRun' or or '-Drdfio.pipeline.resumeAtSavepointId=[(pipelineId:)savepointId,...]')"
                                                .formatted(pipeline.getId()));
                    }
                }
            }

            // Assign default savepoint IDs
            int savepointCount = 0;
            for (Step step : pipeline.getSteps()) {
                if (step instanceof SavepointStep savepoint) {
                    savepointCount++;
                    if (savepoint.getId() == null) {
                        savepoint.setId(String.format("sp%03d", savepointCount));
                    }
                }
            }
            dataset = DatasetFactory.create();
            PipelineState state =
                    new PipelineState(
                            pipeline.getId(),
                            pipeline.getBaseDir(),
                            new RelativePath(workBaseDir, "rdfio").subDir("pipelines"),
                            getLog(),
                            pipeline.getMetadataGraph(),
                            null);
            updatePipelineState(state, project);
            state.setAllowLoadingFromSavepoint(!pipeline.isForceRun());

            int startIndex = -1;
            List<String> stepHashes = new ArrayList<>();

            if (!pipeline.isForceRun()) {
                // Compute hashes for all steps
                String previousHash = "";
                for (Step step : pipeline.getSteps()) {
                    String hash = step.calculateHash(previousHash, state);
                    stepHashes.add(hash);
                    previousHash = hash;
                }
                if (resumeAtSavepointId != null) {
                    String[] savepointIdsArr = resumeAtSavepointId.split(",");
                    Set<String> savepointIds =
                            Arrays.stream(savepointIdsArr)
                                    .filter(
                                            sp ->
                                                    !sp.contains(":")
                                                            || sp.startsWith(
                                                                    pipeline.getId() + ":"))
                                    .map(sp -> sp.replaceFirst("$[^:]+:", ""))
                                    .collect(Collectors.toSet());
                    // exclude all savepoints of other pipelines and remove any pipeline prefix
                    resumeAtSavepointId = savepointIds.stream().collect(Collectors.joining(","));
                    getLog().info(
                                    "Attempting to resume pipeline execution at <savepoint>(s) %s (as instructed via property rdfio.pipeline.resumeAtSavepointId) ..."
                                            .formatted(resumeAtSavepointId));

                    int resumeSavepointIndex = -1;
                    SavepointStep resumeSavepoint = null;
                    for (int i = 0; i < pipeline.getSteps().size(); i++) {
                        Step step = pipeline.getSteps().get(i);
                        if (step instanceof SavepointStep) {
                            if (savepointIds.contains(((SavepointStep) step).getId())) {
                                resumeSavepoint = (SavepointStep) step;
                                resumeSavepointIndex = i;
                                break;
                            }
                        }
                    }
                    if (resumeSavepoint != null) {
                        if (!resumeSavepoint.isEnabled()) {
                            getLog().warn(
                                            "<savepoint> %s (step %d/%d) is not enabled, using standard <savepoint> selection algorithm (latest valid <savepoint>)"
                                                    .formatted(
                                                            resumeAtSavepointId,
                                                            resumeSavepointIndex,
                                                            pipeline.getSteps().size()));
                        }
                        if (resumeSavepoint.isValid(state, stepHashes.get(resumeSavepointIndex))) {
                            getLog().info(
                                            "<savepoint> %s (step %d/%d) is valid, resuming pipeline execution"
                                                    .formatted(
                                                            resumeAtSavepointId,
                                                            resumeSavepointIndex,
                                                            pipeline.getSteps().size()));
                            startIndex = resumeSavepointIndex;
                        } else {
                            getLog().info(
                                            "<savepoint> %s (step %d/%d) is not valid, using standard <savepoint> selection algorithm (latest valid <savepoint>)"
                                                    .formatted(
                                                            resumeAtSavepointId,
                                                            resumeSavepointIndex,
                                                            pipeline.getSteps().size()));
                        }
                    } else {
                        getLog().warn(
                                        "No <savepoint> with id(s) %s found, using standard <savepoint> selection algorithm (latest valid <savepoint>)"
                                                .formatted(resumeAtSavepointId));
                    }
                }
                // standard savepoint selection (latest valid savepoint)
                // if user-provided resumeAtSavepointId was invalid
                if (startIndex == -1) {
                    // Check savepoints in reverse order
                    for (int i = pipeline.getSteps().size() - 1; i >= 0; i--) {
                        Step step = pipeline.getSteps().get(i);
                        if (step instanceof SavepointStep savepoint) {
                            if (savepoint.isValid(state, stepHashes.get(i))) {
                                startIndex = i; // Start with the valid savepoint
                                getLog().info(
                                                "Latest valid <savepoint> is '%s' (step %d/%d) ..."
                                                        .formatted(
                                                                savepoint.getId(),
                                                                (i + 1),
                                                                pipeline.getSteps().size(),
                                                                pipeline.getId()));
                                if (i + 1 < pipeline.getSteps().size()) {
                                    getLog().info("Resuming pipeline at savepoint");
                                } else {
                                    getLog().info(
                                                    "Savepoint is at end of pipeline, nothing to do.");
                                    return;
                                }
                                break;
                            }
                        }
                    }
                }
                if (startIndex == -1 || startIndex == 0) {
                    getLog().info("No valid savepoint found, running the whole pipeline");
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
                previousHash = step.calculateHash(previousHash, state);
                step.executeAndWrapException(dataset, state);
            }
        } catch (Throwable throwable) {
            throw new MojoExecutionException("Error executing PipelineMojo", throwable);
        }
    }

    private void updatePipelineState(PipelineState state, MavenProject project) {
        if (project == null) {
            return;
        }
        Properties properties = project.getProperties();
        String logSeverity = properties.getProperty("shacl.severity.log");
        if (logSeverity != null) {
            state.setDefaultShaclLogSeverity(logSeverity);
        }
        String failSeverity = properties.getProperty("shacl.severity.fail");
        if (failSeverity != null) {
            state.setDefaultShaclFailSeverity(failSeverity);
        }
    }

    void parseConfiguration() throws ConfigurationParseException, MojoExecutionException {
        if (configuration == null) {
            configuration = getExecutionConfiguration();
        }
        try {
            ParsingHelper.requiredDomChild(
                    configuration,
                    "pipeline",
                    Pipeline.makeParser(baseDir, RDFIO.metadataGraph.toString()),
                    this::setPipeline,
                    Pipeline::usage);
        } catch (ConfigurationParseException e) {
            if (e.getConfiguration() != null) {
                throw new MojoExecutionException(
                        "Error parsing <pipeline> at element:\n%s\n\n%s"
                                .formatted(
                                        skipFirstLine(e.getConfiguration().toString()),
                                        e.getMessage()),
                        e);
            } else {
                throw new MojoExecutionException(
                        "Error parsing pipeline: %s (no configuration xml provided)"
                                .formatted(e.getMessage()));
            }
        }
    }

    private static String skipFirstLine(String s) {
        List<String> lines = new ArrayList<>(Arrays.asList(s.split("\n")));
        lines.remove(0);
        return lines.stream().collect(Collectors.joining("\n"));
    }

    /** Package-private getter for testing purposes. */
    Dataset getDataset() {
        return dataset;
    }

    Pipeline getPipeline() {
        return this.pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public void setProject(MavenProject project) {
        this.project = project;
    }

    public void setBaseDir(File baseDir) {
        this.baseDir = baseDir;
    }

    public void setWorkBaseDir(File workBaseDir) {
        this.workBaseDir = workBaseDir;
    }

    public void setConfiguration(Xpp3Dom configuration) {
        this.configuration = configuration;
    }
}
