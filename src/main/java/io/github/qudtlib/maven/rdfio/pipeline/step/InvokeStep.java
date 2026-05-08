package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Executes the steps registered by a {@link StepDefStep} with the given id.
 *
 * <p>Example: {@code <invoke stepRef="refresh-extension-aggregate"/>}
 *
 * <p>The referenced {@code <stepDef>} must have been registered (i.e. executed) before this {@code
 * <invoke>} runs.
 */
public class InvokeStep implements Step {
    private String stepRef;

    public String getStepRef() {
        return stepRef;
    }

    public void setStepRef(String stepRef) {
        this.stepRef = stepRef;
    }

    @Override
    public String getElementName() {
        return "invoke";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (stepRef == null) {
            throw new MojoExecutionException("<invoke> requires a stepRef attribute");
        }
        List<Step> steps = state.getStepDef(stepRef);
        if (steps == null) {
            throw new MojoExecutionException(
                    "<invoke stepRef=\"%s\">: no <stepDef id=\"%s\"> has been registered. Ensure the <stepDef> appears before <invoke> in the pipeline."
                            .formatted(stepRef, stepRef));
        }
        state.log()
                .info(
                        "invoking <stepDef id=\"%s\"> (%d step(s))"
                                .formatted(stepRef, steps.size()),
                        1);
        for (Step step : steps) {
            step.executeAndWrapException(dataset, state);
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("invoke".getBytes(StandardCharsets.UTF_8));
            if (stepRef != null) {
                digest.update(stepRef.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static InvokeStep parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config, "<invoke> configuration is missing.\n" + usage());
        }
        InvokeStep step = new InvokeStep();
        String stepRef = config.getAttribute("stepRef");
        if (stepRef == null || stepRef.trim().isEmpty()) {
            throw new ConfigurationParseException(
                    config, "<invoke> requires a 'stepRef' attribute.\n" + usage());
        }
        step.setStepRef(stepRef.trim());
        return step;
    }

    public static String usage() {
        return """
                Usage: <invoke stepRef="myDef"/>
                Executes the steps registered by <stepDef id="myDef">.
                The referenced <stepDef> must appear (and have executed) before this <invoke>.
                Example:
                <invoke stepRef="refresh-extension-aggregate"/>""";
    }
}
