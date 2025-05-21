package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.pipeline.step.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Pipeline {
    private String id;

    private String metadataGraph;

    private boolean forceRun;

    private File baseDir;

    private List<Step> steps = new ArrayList<>();

    public void addAddStep(AddStep step) {
        this.steps.add(step);
    }

    public void addForeachStep(ForeachStep step) {
        this.steps.add(step);
    }

    public void addSavepointStep(SavepointStep step) {
        this.steps.add(step);
    }

    public void addShaclInferStep(ShaclInferStep step) {
        this.steps.add(step);
    }

    public void addSparqlUpdateStep(SparqlUpdateStep step) {
        this.steps.add(step);
    }

    public void addWriteStep(WriteStep step) {
        this.steps.add(step);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    // Pipeline.java
    public static Pipeline parse(Xpp3Dom config, File defaultBaseDir, String defaultMetadataGraph)
            throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Pipeline configuration is missing.
                            Usage: Provide a <pipeline> element with required <id> and optional <metadataGraph>, <forceRun>, <baseDir>, and <steps>.
                            Example:
                            <pipeline>
                                <id>my-pipeline</id>
                                <steps>...</steps>
                            </pipeline>""");
        }

        Pipeline pipeline = new Pipeline();
        Xpp3Dom idDom = config.getChild("id");
        if (idDom == null || idDom.getValue() == null || idDom.getValue().trim().isEmpty()) {
            throw new MojoExecutionException(
                    """
                            Pipeline ID is required.
                            Usage: Specify a non-empty <id> in the <pipeline> element.
                            Example: <id>my-pipeline</id>""");
        }
        pipeline.setId(idDom.getValue().trim());

        Xpp3Dom metadataGraphDom = config.getChild("metadataGraph");
        pipeline.setMetadataGraph(
                metadataGraphDom != null && metadataGraphDom.getValue() != null
                        ? metadataGraphDom.getValue().trim()
                        : defaultMetadataGraph);

        Xpp3Dom forceRunDom = config.getChild("forceRun");
        pipeline.setForceRun(
                forceRunDom != null
                        && forceRunDom.getValue() != null
                        && Boolean.parseBoolean(forceRunDom.getValue().trim()));

        Xpp3Dom baseDirDom = config.getChild("baseDir");
        pipeline.setBaseDir(
                baseDirDom != null && baseDirDom.getValue() != null
                        ? new File(baseDirDom.getValue().trim())
                        : defaultBaseDir);

        List<Step> steps = new ArrayList<>();
        Xpp3Dom stepsDom = config.getChild("steps");
        if (stepsDom != null) {
            for (Xpp3Dom stepDom : stepsDom.getChildren()) {
                String stepType = stepDom.getName();
                Step step =
                        switch (stepType) {
                            case "add" -> AddStep.parse(stepDom);
                            case "sparqlUpdate" -> SparqlUpdateStep.parse(stepDom);
                            case "savepoint" -> SavepointStep.parse(stepDom);
                            case "shaclInfer" -> ShaclInferStep.parse(stepDom);
                            case "write" -> WriteStep.parse(stepDom);
                            case "foreach" -> ForeachStep.parse(stepDom);
                            default ->
                                    throw new MojoExecutionException(
                                            "Unknown step type: "
                                                    + stepType
                                                    + ".\n"
                                                    + "Usage: Use one of: <add>, <sparqlUpdate>, <savepoint>, <shaclInfer>, <write>, <foreach>.\n"
                                                    + "Example: <add><file>data.ttl</file><toGraph>test:graph</toGraph></add>");
                        };
                steps.add(step);
            }
        }
        pipeline.setSteps(steps);
        return pipeline;
    }
}
