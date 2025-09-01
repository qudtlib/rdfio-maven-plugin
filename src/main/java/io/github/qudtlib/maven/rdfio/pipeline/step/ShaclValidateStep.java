package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.file.ShaclHelper;
import io.github.qudtlib.maven.rdfio.pipeline.FileAccess;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.InputsComponent;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ResultSeverityConfig;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ValidationReportComponent;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import io.github.qudtlib.maven.rdfio.pipeline.support.PipelineConfigurationExeception;
import io.github.qudtlib.maven.rdfio.pipeline.support.VariableResolver;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.shacl.validation.Severity;
import org.apache.jena.shacl.vocabulary.SHACL;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.topbraid.shacl.validation.ValidationEngineConfiguration;
import org.topbraid.shacl.validation.ValidationUtil;

public class ShaclValidateStep implements Step {

    private static final String SEVERITY_NONE = "NONE";

    private String message;

    private Boolean failOnMissingInputGraph = true;

    private InputsComponent<ShaclValidateStep> shapes;

    private InputsComponent<ShaclValidateStep> data;

    private ValidationReportComponent validationReportComponent;

    private String failOnSeverity = "Violation";

    private ResultSeverityConfig failOnSeverityParsed = null;

    private String logSeverity = "Violation";

    private ResultSeverityConfig logSeverityParsed = null;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Boolean isfailOnMissingInputGraph() {
        return failOnMissingInputGraph;
    }

    public void setFailOnMissingInputGraph(Boolean failOnMissingInputGraph) {
        this.failOnMissingInputGraph = failOnMissingInputGraph;
    }

    public InputsComponent<ShaclValidateStep> getShapes() {
        return shapes;
    }

    public void setShapes(InputsComponent<ShaclValidateStep> shapes) {
        this.shapes = shapes;
    }

    public InputsComponent<ShaclValidateStep> getData() {
        return data;
    }

    public void setData(InputsComponent<ShaclValidateStep> data) {
        this.data = data;
    }

    public String getLogSeverity() {
        return logSeverity;
    }

    public void setLogSeverity(String logSeverity) {
        this.logSeverity = logSeverity;
    }

    public ValidationReportComponent getValidationReportComponent() {
        return validationReportComponent;
    }

    public void setValidationReportComponent(ValidationReportComponent validationReportComponent) {
        this.validationReportComponent = validationReportComponent;
    }

    public String getFailOnSeverity() {
        return failOnSeverity;
    }

    public void setFailOnSeverity(String failOnSeverity) {
        this.failOnSeverity = failOnSeverity;
    }

    public static ShaclValidateStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    ShaclInfer step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        ShaclValidateStep step = new ShaclValidateStep();

        // Parse optional message
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, ShaclValidateStep::usage);
        ParsingHelper.optionalBooleanChild(
                config,
                "failOnMissingInputGraph",
                step::setFailOnMissingInputGraph,
                ShaclValidateStep::usage);
        ParsingHelper.optionalDomChild(
                config,
                "shapes",
                InputsComponent.getParseFunction(step),
                step::setShapes,
                ShaclValidateStep::usage);
        ParsingHelper.optionalDomChild(
                config,
                "data",
                InputsComponent.getParseFunction(step),
                step::setData,
                ShaclValidateStep::usage);
        ParsingHelper.optionalDomChild(
                config,
                "validationReport",
                ValidationReportComponent.getParser(step),
                step::setValidationReportComponent,
                ShaclValidateStep::usage);
        if (step.getShapes() == null) {
            throw new ConfigurationParseException(
                    config, "<shaclInfer> must have a <shapes> sub-element.\n" + usage());
        }
        if (step.getData() == null) {
            throw new ConfigurationParseException(
                    config, "<shaclInfer> must have a <data> sub-element.\n" + usage());
        }
        ParsingHelper.optionalStringChild(
                config, "failOnSeverity", step::setFailOnSeverity, ShaclValidateStep::usage);
        ParsingHelper.optionalStringChild(
                config, "logSeverity", step::setLogSeverity, ShaclValidateStep::usage);

        step.failOnSeverityParsed = ResultSeverityConfig.valueOf(step.failOnSeverity);
        step.logSeverityParsed = ResultSeverityConfig.valueOf(step.logSeverity);
        return step;
    }

    public static String usage() {
        return """
                Usage: Specify
                    - <message> (optional): a description of the validate step
                    - <shapes>: <file>, <files>, <graph>, or <graphs> for SHACL shapes (none to use the default graph
                                as the shapes graph)
                    - <data>: data sources via <file>, <files>, <graph>, or <graphs> (none to use the default graph
                              as the data graph)
                    - <failOnMissingInputGraph> (default: true): if false, a specified <graph> that is not present in
                            the dataset (which is the case if that graph was added but is empty as well as if it was not
                            added) does not cause a build failure
                    - <failForSeverity>: severity, one of Info, Warn, Violation or None if the validation should
                                         not make the build fail. Default: Violation
                    - <logSeverity>: lowest severity (Info < Warn < Violation < None) to print in the maven output on
                                     log level info. Default: Violation
                    - <validationReport>: output the validation report (an set of triples) via <graph> and/or <file>
                                          - leave empty to write the report to the default graph
                                          - omit the element - no validation report is written
                    NOTE: shacl functions loaded with <shaclFunctions> can be used in the shapes
                Examples:
                 - <shaclValidate>
                       <message>Inferring triples</message>
                       <shapes><file>shapes.ttl</file></shapes>
                       <data><files><include>data/*.ttl</include></files></data>
                       <failForSeverity>Warn</failForSeverity>
                       <logSeverity>Info</logSeverity>
                       <validationReport><graph>validation:graph</graph></inferred>
                   </shaclValidate>
                 - <shaclValidate>
                       <shapes><graph>shapes:graph</graph></shapes>
                       <data><graph>data:graph</graph></data>
                       <validationReport><file>target/inferred.ttl</file></validationReport>
                   </shaclValidate>
                 - <shaclValidate/> - use the default graph for data and shapes, no validation report
               """;
    }

    @Override
    public String getElementName() {
        return "shaclValidate";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (message != null) {
            state.log().info(state.variables().resolve(message, dataset));
        }
        try {
            Model shapesModel = populateShapesModel(dataset, state);
            Model dataModel = populateDataModel(dataset, state);
            long start = System.currentTimeMillis();
            List<String> iterationTriplesFiles = new ArrayList<>();
            Resource validationReport =
                    ValidationUtil.validateModel(
                            dataModel,
                            shapesModel,
                            new ValidationEngineConfiguration()
                                    .setReportDetails(true)
                                    .setValidateShapes(false));
            state.log().info("ValidationReport:", 1);
            ValidationReportSummary summary =
                    summarizeValidationReport(
                            validationReport.getModel(), System.currentTimeMillis() - start);
            List<String> validationStats = formatValidationReportSummary(summary);
            state.log().info(validationStats, 2);
            state.log().info("Output:", 1);
            if (validationReportComponent != null) {
                boolean outputToDefaultGraph = true;
                if (validationReportComponent.getGraph() != null) {
                    String graphName =
                            VariableResolver.resolveVariables(
                                    validationReportComponent.getGraph(),
                                    dataset,
                                    state.getMetadataGraph());
                    dataset.addNamedModel(graphName, validationReport.getModel());
                    state.log().info(String.format("%5s: %s", "graph", graphName), 2);
                    PipelineHelper.bindGraphToNoFileIfUnbound(dataset, state, graphName);
                    outputToDefaultGraph = false;
                }
                if (validationReportComponent.getFile() != null) {
                    RelativePath path =
                            state.files()
                                    .make(
                                            VariableResolver.resolveVariables(
                                                    validationReportComponent.getFile(),
                                                    dataset,
                                                    state.getMetadataGraph()));
                    state.files().writeRdf(path, validationReport.getModel());
                    state.log().info(String.format("%5s: %s", "file", path.getRelativePath()), 2);
                    outputToDefaultGraph = false;
                }
                if (outputToDefaultGraph) {
                    state.log()
                            .info(
                                    String.format(
                                            "%5s: %s",
                                            "graph", PipelineHelper.formatDefaultGraph()),
                                    2);
                    dataset.getDefaultModel().add(validationReport.getModel());
                }
            }
            if (summary.buildFails()) {
                String report =
                        ShaclHelper.formatValidationReport(summary.filteredValidationReportModel());
                state.log().info("");
                state.log().info(report, 2);
                state.log().info("");
                throw new MojoExecutionException(
                        String.format("SHACL validation failed - see log output"));
            }
            state.getPrecedingSteps().add(this);
        } catch (RuntimeException e) {
            throw new MojoExecutionException(
                    "Error executing ShaclInferStep\nProblem: %s\n%s"
                            .formatted(e.getMessage(), usage()),
                    e);
        }
    }

    private ValidationReportSummary summarizeValidationReport(Model model, long millis) {
        ValidationReport jenaValidationReport =
                org.apache.jena.shacl.ValidationReport.fromModel(model);
        Model filteredModel = filterModel(model);
        ValidationReport jenaValidationReportFiltered =
                org.apache.jena.shacl.ValidationReport.fromModel(filteredModel);
        long numResults =
                countReports(
                        jenaValidationReport, Severity.Violation, Severity.Warning, Severity.Info);
        long numResultsFiltered =
                countReports(
                        jenaValidationReportFiltered,
                        Severity.Violation,
                        Severity.Warning,
                        Severity.Info);
        long violations = countReports(jenaValidationReport, Severity.Violation);
        long filteredViolations = countReports(jenaValidationReportFiltered, Severity.Violation);
        long warnings = countReports(jenaValidationReport, Severity.Warning);
        long filteredWarnings = countReports(jenaValidationReportFiltered, Severity.Warning);
        long infos = countReports(jenaValidationReport, Severity.Info);
        long filteredInfos = countReports(jenaValidationReportFiltered, Severity.Info);
        boolean buildFails = isBuildFails(jenaValidationReport);
        return new ValidationReportSummary(
                millis,
                numResults,
                numResultsFiltered,
                infos,
                filteredInfos,
                warnings,
                filteredWarnings,
                violations,
                filteredViolations,
                filteredModel,
                buildFails);
    }

    private List<String> formatValidationReportSummary(ValidationReportSummary summary) {
        List<String> ret = new ArrayList<>();
        ret.add(
                "    logSeverity: "
                        + this.logSeverityParsed
                        + " (change using property shacl.severity.log)");
        ret.add(
                " failOnSeverity: "
                        + this.failOnSeverityParsed
                        + " (change using property shacl.severity.fail)");
        ret.add(
                "        results: "
                        + summary.numResults
                        + (summary.numResults() != summary.numResultsFiltered()
                                ? " (filtered: %s )".formatted(summary.numResultsFiltered())
                                : ""));
        ret.add(
                " severity Violation: "
                        + summary.numViolations()
                        + (summary.numViolations() != summary.numViolationsFiltered()
                                ? " (filtered: %s )".formatted(summary.numViolationsFiltered())
                                : ""));
        ret.add(
                "   severity Warning: "
                        + summary.numWarn()
                        + (summary.numWarn() != summary.numWarnFiltered()
                                ? " (filtered: %s )".formatted(summary.numWarn())
                                : ""));
        ret.add(
                "      severity Info: "
                        + summary.numInfo()
                        + (summary.numInfo() != summary.numInfoFiltered()
                                ? " (filtered: %s )".formatted(summary.numInfo())
                                : ""));
        return ret;
    }

    private Model populateDataModel(Dataset dataset, PipelineState state)
            throws MojoExecutionException {
        return populateModelFromInputs(
                dataset,
                state,
                this.data,
                List.of(),
                "SHACL data",
                this.isfailOnMissingInputGraph());
    }

    private Model populateShapesModel(Dataset dataset, PipelineState state) {
        Model shapesModel =
                populateModelFromInputs(
                        dataset,
                        state,
                        this.shapes,
                        List.of(state.getShaclFunctionsGraph()),
                        "SHACL shapes",
                        isfailOnMissingInputGraph());
        return shapesModel;
    }

    private static Model populateModelFromInputs(
            Dataset dataset,
            PipelineState state,
            InputsComponent<ShaclValidateStep> inputsComponent,
            List<String> additionalGraphs,
            String fileKind,
            boolean failOnMissingInputGraph) {
        Model dataModel = ModelFactory.createDefaultModel();
        List<String> entries = new ArrayList<>();
        if (inputsComponent == null || inputsComponent.hasNoInputs()) {
            dataModel.add(dataset.getDefaultModel());
            entries.add(PipelineHelper.formatDefaultGraph());
        } else {
            List<RelativePath> dataPaths = inputsComponent.getAllInputPaths(dataset, state);
            FileHelper.ensureRelativePathsExist(dataPaths, fileKind);
            FileAccess.readRdf(dataPaths, dataModel, state);
            entries.addAll(PipelineHelper.formatPaths(dataPaths));
            List<String> allGraphs = new ArrayList<>();
            allGraphs.addAll(additionalGraphs);
            allGraphs.addAll(inputsComponent.getAllInputGraphs(dataset, state));
            if (allGraphs != null) {
                allGraphs.forEach(
                        g -> {
                            if (!dataset.containsNamedModel(g)
                                    && !state.getShaclFunctionsGraph().equals(g)
                                    && failOnMissingInputGraph) {
                                throw new PipelineConfigurationExeception(
                                        "No graph %s found in dataset, cannot use in shaclValidate"
                                                .formatted(g));
                            }
                            dataModel.add(dataset.getNamedModel(g));
                        });
            }
            entries.addAll(PipelineHelper.formatGraphs(allGraphs));
        }
        state.log().info("    " + fileKind);
        state.log().info(entries, 2);
        return dataModel;
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("shaclInfer".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            if (shapes != null) {
                shapes.updateHash(digest, state);
            }
            if (data != null) {
                data.updateHash(digest, state);
            }
            if (validationReportComponent != null) {
                if (validationReportComponent.getGraph() != null) {
                    digest.update(
                            validationReportComponent.getGraph().getBytes(StandardCharsets.UTF_8));
                }
                if (validationReportComponent.getFile() != null) {
                    digest.update(
                            validationReportComponent.getFile().getBytes(StandardCharsets.UTF_8));
                }
            }
            digest.update(String.valueOf(failOnSeverity).getBytes(StandardCharsets.UTF_8));
            digest.update(String.valueOf(logSeverity).getBytes(StandardCharsets.UTF_8));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    private Model filterModel(Model model) {
        Graph originalGraph = model.getGraph();
        Graph copyGraph = GraphFactory.createGraphMem();
        originalGraph.stream().forEach(copyGraph::add);
        Model filtered = ModelFactory.createModelForGraph(copyGraph);
        filtered.setNsPrefixes(model.getNsPrefixMap());
        List<Resource> results =
                filtered.listStatements(
                                (Resource) null,
                                RDF.type,
                                filtered.asRDFNode(SHACL.ValidationResult))
                        .mapWith(Statement::getSubject)
                        .toList();
        List<Resource> resultsToDelete = new ArrayList<>();
        Property resultSeverityProp =
                filtered.createProperty(
                        filtered.asRDFNode(SHACL.resultSeverity).asResource().getURI());
        for (Resource result : results) {
            List<RDFNode> severityList =
                    filtered.listStatements(result, resultSeverityProp, (RDFNode) null)
                            .mapWith(Statement::getObject)
                            .toList();
            Resource severityRes = severityList.get(0).asResource();
            ResultSeverityConfig shaclResultSeverity =
                    ResultSeverityConfig.valueOf(severityRes.getLocalName());
            if (!shaclResultSeverity.isHigherThanOrEqualTo(this.logSeverityParsed)) {
                resultsToDelete.add(result);
            }
        }

        if (!resultsToDelete.isEmpty()) {
            for (Resource toDelete : resultsToDelete) {
                filtered.removeAll(toDelete, null, null);
                filtered.removeAll(null, null, toDelete);
            }
        }
        return filtered;
    }

    private boolean isBuildFails(ValidationReport validationReport) {
        if (failOnSeverityParsed == ResultSeverityConfig.None) {
            return false;
        }
        if (failOnSeverityParsed == ResultSeverityConfig.Info) {
            return validationReport.conforms();
        } else if (failOnSeverityParsed == ResultSeverityConfig.Warning) {
            return (countReports(validationReport, Severity.Warning, Severity.Violation) > 0);
        } else if (failOnSeverityParsed == ResultSeverityConfig.Violation) {
            return (countReports(validationReport, Severity.Violation) > 0);
        }
        throw new IllegalStateException(
                String.format(
                        "Cannot handle value '%s' of parameter'%s'",
                        failOnSeverity, "'failOnSeverity'"));
    }

    private long countReports(ValidationReport validationReportComponent, Severity... severities) {
        return validationReportComponent.getEntries().stream()
                .filter(e -> Arrays.stream(severities).anyMatch(s -> e.severity() == s))
                .count();
    }

    private record ValidationReportSummary(
            long durationInMillis,
            long numResults,
            long numResultsFiltered,
            long numInfo,
            long numInfoFiltered,
            long numWarn,
            long numWarnFiltered,
            long numViolations,
            long numViolationsFiltered,
            Model filteredValidationReportModel,
            boolean buildFails) {}
}
