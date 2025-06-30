package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class UntilStep implements Step {
    private String sparqlAsk;
    private String indexVar;
    private String message;

    private final List<Step> body = new ArrayList<>();

    public List<Step> getBody() {
        return body;
    }

    public String getSparqlAsk() {
        return sparqlAsk;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setSparqlAsk(String sparqlAsk) {
        this.sparqlAsk = sparqlAsk;
    }

    public String getIndexVar() {
        return indexVar;
    }

    public void setIndexVar(String indexVar) {
        this.indexVar = indexVar;
    }

    public void addBodyStep(Step step) {
        this.body.add(step);
    }

    @Override
    public String getElementName() {
        return "until";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (sparqlAsk == null || body.isEmpty()) {
            throw new MojoExecutionException("SparqlAsk and body are required in foreach step");
        }
        if (message != null) {
            state.log().info(message, 1);
        }
        if (indexVar != null) {
            state.log().info("index variable: " + indexVar, 1);
        }
        boolean[] result = new boolean[] {false};
        int i = 0;
        try {
            state.incIndentLevel();
            do {
                i++;
                state.log().info("<until> iteration %d: entering body".formatted(i));
                state.log().info("");
                if (indexVar != null) {
                    PipelineHelper.setPipelineVariable(
                            dataset, state, indexVar, ResourceFactory.createTypedLiteral(i));
                    state.log().info("variable %s=%s".formatted(this.indexVar, i), 1);
                }
                for (Step step : body) {
                    step.executeAndWrapException(dataset, state);
                }
                String sparqlAskString = this.sparqlAsk;
                SparqlHelper.executeSparqlQueryWithVariables(
                        sparqlAskString,
                        dataset,
                        state.getMetadataGraph(),
                        new SparqlHelper.QueryResultProcessor() {
                            @Override
                            public void processAskResult(boolean askResult) {
                                result[0] = askResult;
                            }
                        });
                state.log().info("");
                state.log()
                        .info(
                                "<until> iteration %d: ask query yields %s"
                                        .formatted(i, Boolean.toString(result[0])));
            } while (!result[0]);
        } finally {
            state.decIndentLevel();
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("until".getBytes(StandardCharsets.UTF_8));
            if (sparqlAsk != null) {
                digest.update(sparqlAsk.getBytes(StandardCharsets.UTF_8));
            }
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            if (indexVar != null) {
                digest.update(indexVar.getBytes(StandardCharsets.UTF_8));
            }
            String subPreviousHash = "";
            for (Step subStep : body) {
                subPreviousHash = subStep.calculateHash(subPreviousHash, state);
                digest.update(subPreviousHash.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    // untilStep.java
    public static UntilStep parse(Xpp3Dom config) {
        UntilStep step = new UntilStep();
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Until step configuration is missing.
                            %s"""
                            .formatted(step.usage()));
        }

        ParsingHelper.requiredStringChild(config, "sparqlAsk", step::setSparqlAsk, step::usage);
        ParsingHelper.optionalStringChild(config, "indexVar", step::setIndexVar, step::usage);
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, step::usage);
        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Until step requires a <body> with a single step.
                            %s"""
                            .formatted(step.usage()));
        }
        for (Xpp3Dom bodyStepConfig : bodyDom.getChildren()) {
            String bodyStepType = bodyStepConfig.getName();
            Step bodyStep =
                    switch (bodyStepType) {
                        case "add" -> AddStep.parse(bodyStepConfig);
                        case "assert" -> AssertStep.parse(bodyStepConfig);
                        case "clear" -> ClearStep.parse(bodyStepConfig);
                        case "foreach" -> ForeachStep.parse(bodyStepConfig);
                        case "shaclInfer" -> ShaclInferStep.parse(bodyStepConfig);
                        case "shaclValidate" -> ShaclValidateStep.parse(bodyStepConfig);
                        case "sparqlQuery" -> SparqlQueryStep.parse(bodyStepConfig);
                        case "sparqlUpdate" -> SparqlUpdateStep.parse(bodyStepConfig);
                        case "until" -> UntilStep.parse(bodyStepConfig);
                        case "write" -> WriteStep.parse(bodyStepConfig);
                        default ->
                                throw new ConfigurationParseException(
                                        config,
                                        "Invalid step type in until <body>: "
                                                + bodyStepType
                                                + ".\n"
                                                + "Usage: Use one of: <add>, <sparqlUpdate>, <shaclInfer>, <shaclValidate>, <write>, <foreach>, <until>.\n"
                                                + "Example: <body><sparqlUpdate><sparql>...</sparql></sparqlUpdate></body>");
                    };
            step.addBodyStep(bodyStep);
        }

        return step;
    }

    public String usage() {
        return """
                            Until step configuration is missing.
                            Usage: Provide a <until> element with <var>, <values>, and <body>.
                            Example:
                            <until>
                                <indexVar>fileGraph</indexVar>
                                <sparqlAsk>[break condition]</sparqlAsk>
                                <body><sparqlUpdate>...</sparqlUpdate></body>
                            </until>""";
    }
}
