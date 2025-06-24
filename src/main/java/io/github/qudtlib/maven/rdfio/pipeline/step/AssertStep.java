package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class AssertStep implements Step {
    private String sparqlSelect;
    private String message = null;
    private String failureMessage = null;
    private boolean failBuild = true;

    public String getSparqlSelect() {
        return sparqlSelect;
    }

    public void setSparqlSelect(String sparqlSelect) {
        this.sparqlSelect = sparqlSelect;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public boolean isFailBuild() {
        return failBuild;
    }

    public void setFailBuild(boolean failBuild) {
        this.failBuild = failBuild;
    }

    @Override
    public String getElementName() {
        return "assert";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state)
            throws RuntimeException, MojoExecutionException {
        if (this.message != null) {
            state.log().info(this.message);
        }
        final boolean[] solutionsPresent = new boolean[1];
        final String[] querysolutions = new String[1];
        var resultProcessor =
                new SparqlHelper.QueryResultProcessor() {
                    @Override
                    public void processSelectResult(ResultSet result) {
                        if (result.hasNext()) {
                            solutionsPresent[0] = true;
                        }
                        querysolutions[0] = ResultSetFormatter.asText(result);
                    }
                };
        SparqlHelper.executeSparqlQueryWithVariables(
                this.sparqlSelect, dataset, state.getMetadataGraph(), resultProcessor);
        if (solutionsPresent[0]) {
            String msg =
                    """

                    %s

                    The provided <sparqlQuery> should not yield any solutions, but the following solutions were found:

                    %s

                    """
                            .formatted(
                                    this.failureMessage == null ? "" : this.failureMessage,
                                    querysolutions[0]);

            state.log().info(msg, 1);
            if (this.failBuild) {
                throw new MojoExecutionException(msg);
            }
        } else {
            state.log().info("Assertion successful: provided <sparqlQuery> yields no results", 1);
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("assert".getBytes(StandardCharsets.UTF_8));
            if (this.sparqlSelect != null) {
                digest.update(this.sparqlSelect.getBytes(StandardCharsets.UTF_8));
            }
            if (this.message != null) {
                digest.update(this.message.getBytes(StandardCharsets.UTF_8));
            }
            if (this.failureMessage != null) {
                digest.update(this.failureMessage.getBytes(StandardCharsets.UTF_8));
            }
            digest.update((Boolean.toString(this.failBuild).getBytes(StandardCharsets.UTF_8)));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static AssertStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }
        AssertStep step = new AssertStep();
        ParsingHelper.optionalStringChild(
                config, "sparqlSelect", step::setSparqlSelect, AssertStep::usage);
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, AssertStep::usage);
        ParsingHelper.optionalStringChild(
                config, "failureMessage", step::setFailureMessage, AssertStep::usage);
        ParsingHelper.optionalBooleanChild(
                config, "failBuild", step::setFailBuild, AssertStep::usage);
        return step;
    }

    public static String usage() {
        return """
                           Usage:

                           Fails the build if the nested SPARQL SELECT query has any solutions
                           - sparqlSelect: the SPARQL SELECT query to execute
                           - message (optional): message to display in the log
                           - failureMessage (optional): message to use if the assertion fails
                           - failBuild (optional, default true): build fails if assertion fails

                           Example:
                           <assert>
                            <message>Ensure we don't use the forbidden value</message>
                            <failureMessage>We do use the forbidden value!</message>
                            <failBuild>false</failBuild>
                            <sparqlSelect>
                                SELECT * where {
                                    ?s ?p forbidden:value
                                }
                            </sparqlSelect>
                           </assert>
                           """;
    }
}
