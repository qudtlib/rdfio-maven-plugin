package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.step.SavepointStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
    @Parameter(property = "rdfio.pipeline.forceRun", defaultValue = "false")
    private Object forceRun;

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
                if (pipeline.isForceRun()) {
                    getLog().info(
                                    "Pipeline '%s' is configured to run from the start, ignoring any valid savepoints (can be overriddden using maven property '-Drdfio.pipeline.forceRun=false')"
                                            .formatted(pipeline.getId()));
                } else {
                    getLog().info(
                                    "Pipeline '%s' is configured to start from the lastest valid savepoint (can be overriddden using maven property '-Drdfio.pipeline.forceRun')"
                                            .formatted(pipeline.getId()));
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

                // Check savepoints in reverse order
                for (int i = pipeline.getSteps().size() - 1; i >= 0; i--) {
                    Step step = pipeline.getSteps().get(i);
                    if (step instanceof SavepointStep savepoint) {
                        if (savepoint.isValid(state, stepHashes.get(i))) {
                            startIndex = i; // Start with the valid savepoint
                            getLog().info(
                                            "Valid <savepoint> '%s' found (step %d) in pipeline '%s', resuming pipeline execution from there."
                                                    .formatted(
                                                            savepoint.getId(),
                                                            (i + 1),
                                                            pipeline.getId()));
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
                previousHash = step.calculateHash(previousHash, state);
                step.executeAndWrapException(dataset, state);
            }
        } catch (Throwable throwable) {
            throw new MojoExecutionException("Error executing PipelineMojo", throwable);
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
            throw new MojoExecutionException(
                    "Error parsing <pipeline> at element:\n%s\n\n%s"
                            .formatted(
                                    skipFirstLine(e.getConfiguration().toString()), e.getMessage()),
                    e);
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
