package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.pipeline.step.*;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
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

    public static Function<Xpp3Dom, Pipeline> makeParser(
            File defaultBaseDir, String defaultMetadataGraph) {
        return config -> Pipeline.parse(config, defaultBaseDir, defaultMetadataGraph);
    }

    // Pipeline.java
    public static Pipeline parse(Xpp3Dom config, File defaultBaseDir, String defaultMetadataGraph)
            throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Pipeline configuration is missing.
                            %s"""
                            .formatted(Pipeline.usage()));
        }

        Pipeline pipeline = new Pipeline();
        Xpp3Dom idDom = config.getChild("id");
        if (idDom == null || idDom.getValue() == null || idDom.getValue().trim().isEmpty()) {
            throw new ConfigurationParseException(
                    config,
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
                            case "assert" -> AssertStep.parse(stepDom);
                            case "clear" -> ClearStep.parse(stepDom);
                            case "foreach" -> ForeachStep.parse(stepDom);
                            case "savepoint" -> SavepointStep.parse(stepDom);
                            case "shaclFunctions" -> ShaclFunctionsStep.parse(stepDom);
                            case "shaclInfer" -> ShaclInferStep.parse(stepDom);
                            case "shaclValidate" -> ShaclValidateStep.parse(stepDom);
                            case "sparqlQuery" -> SparqlQueryStep.parse(stepDom);
                            case "sparqlUpdate" -> SparqlUpdateStep.parse(stepDom);
                            case "until" -> UntilStep.parse(stepDom);
                            case "write" -> WriteStep.parse(stepDom);
                            default ->
                                    throw new ConfigurationParseException(
                                            config,
                                            "Unknown step type: "
                                                    + stepType
                                                    + ".\n"
                                                    + "Usage: Use one of: <add>, <sparqlUpdate>, <savepoint>, <shaclFunctions>, <shaclInfer>, <shaclValidate>, <write>, <foreach>.\n"
                                                    + "Example: <add><file>data.ttl</file><toGraph>test:graph</toGraph></add>");
                        };
                steps.add(step);
            }
        }
        pipeline.setSteps(steps);
        return pipeline;
    }

    public static String usage() {
        return """
                            A Pipeline uses an in-memory RDF dataset. In the pipelines <step>s, the dataset's graphs can
                            be populated, changed, and written to files.

                            Usage: Provide a <pipeline> element with required <id> and <steps> and optional
                                   <metadataGraph>, <forceRun>, and <baseDir>.
                                    - <metagdataGraph>: the graph in the dataset where metadata (eg which graph
                                                        corresponds to which file) is stored
                                    - <forceRun>: if the pipeline has <savepoints>, the pipeline will check if
                                                   anything has changed since the last run prior to the savepoint.
                                                   <forceRun>true</forceRun> will disable savepoints.
                                    - <baseDir>: the base directory that the pipeline will use. No files can be read or
                                                 written outside of the baseDir.
                                    - <steps>: one of
                                        <add>: add data to a graph (or to the default graph)
                                        <write>: write graph(s) to a file (or files)
                                        <shaclInfer>: infer triples using SHACL-AF
                                        <sparqlUpdate>: add or delete triples in the dataset
                                        <foreach>: iterate over graphs (currently the only thing you can iterate over)
                                        <savepoint>: allow to skip ahead to a savepoint when nohthing has changed
                                                     since the last run
                                        <shaclFunctions>: register RDF data containing SHACL-AF Functions to make them
                                                          available in SPARQL and SHACL validation and inferencing
                            Example:
                            <pipeline>
                                <id>my-pipeline</id>
                                <forceRun>true</forceRun>
                                <steps>
                                    <add>
                                        <file>src/data/schema.ttl</file>
                                        <file>src/data/data.ttl</file>
                                        <toGraph>graph:everything</toGraph> <!-- omit this to load into the default graph -->
                                    </add>
                                    <write>
                                        <graph>graph:everything</graph>  <!-- omit this to write from the default graph -->
                                        <toFile>target/rdf/merged.ttl</toFile>
                                    </write>
                                </steps>
                            </pipeline>""";
    }
}
