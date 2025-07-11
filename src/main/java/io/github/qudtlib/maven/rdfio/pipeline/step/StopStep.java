package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class StopStep implements Step {

    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String getElementName() {
        return "stop";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (message != null) {
            state.log().info(state.variables().resolve(message, dataset), 1);
        }
        state.log().info("Stopping pipeline execution", 1);
        throw new MojoExecutionException(
                "Pipeline stopped by <stop> step" + (message != null ? ": " + message : ""));
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("stop".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static StopStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    Stop step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        StopStep step = new StopStep();
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, StopStep::usage);
        return step;
    }

    public static String usage() {
        return """
                Usage: Provide a <stop> element with an optional <message> subelement.
                The step halts pipeline execution, logging the message if provided.
                Example:
                <stop>
                    <message>Pipeline execution halted due to validation failure</message>
                </stop>
                or
                <stop/>
                """;
    }
}
