package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.Pipeline;
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
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class WhenStep implements Step {
    private String sparqlAsk;
    private String message;
    private final List<Step> body = new ArrayList<>();

    public String getSparqlAsk() {
        return sparqlAsk;
    }

    public void setSparqlAsk(String sparqlAsk) {
        this.sparqlAsk = sparqlAsk;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<Step> getBody() {
        return body;
    }

    public void addBodyStep(Step step) {
        this.body.add(step);
    }

    @Override
    public String getElementName() {
        return "when";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (sparqlAsk == null || body.isEmpty()) {
            throw new MojoExecutionException("sparqlAsk and body are required in when step");
        }
        if (message != null) {
            state.log().info(state.variables().resolve(message, dataset), 1);
        }
        boolean[] result = new boolean[] {false};
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
        state.log().info("SPARQL ASK query result: " + result[0], 1);
        if (result[0]) {
            state.log().info("Condition true, executing body", 1);
            try {
                state.incIndentLevel();
                for (Step step : body) {
                    step.executeAndWrapException(dataset, state);
                }
            } finally {
                state.decIndentLevel();
            }
        } else {
            state.log().info("Condition false, skipping body", 1);
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("when".getBytes(StandardCharsets.UTF_8));
            if (sparqlAsk != null) {
                digest.update(sparqlAsk.getBytes(StandardCharsets.UTF_8));
            }
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
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

    public static WhenStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    When step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        WhenStep step = new WhenStep();
        ParsingHelper.requiredStringChild(config, "sparqlAsk", step::setSparqlAsk, WhenStep::usage);
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, WhenStep::usage);
        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    config,
                    """
                    When step requires a <body> with at least one step.
                    %s"""
                            .formatted(usage()));
        }
        for (Xpp3Dom bodyStepConfig : bodyDom.getChildren()) {
            String bodyStepType = bodyStepConfig.getName();
            Step bodyStep =
                    Pipeline.parseStep(config, bodyStepConfig, bodyStepType, "when", "savepoint");
            step.addBodyStep(bodyStep);
        }

        return step;
    }

    public static String usage() {
        return """
                Usage: Provide an <when> element with a required <sparqlAsk> and <body>, and an optional <message>.
                The <sparqlAsk> is evaluated as a SPARQL ASK query. If true, the steps in <body> are executed.
                Example:
                <when>
                    <message>Checking if data exists before processing</message>
                    <sparqlAsk>
                        ASK { ?s ?p ?o }
                    </sparqlAsk>
                    <body>
                        <sparqlUpdate>
                            <sparql>
                                INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> "value" } }
                            </sparql>
                        </sparqlUpdate>
                    </body>
                </when>
                """;
    }
}
