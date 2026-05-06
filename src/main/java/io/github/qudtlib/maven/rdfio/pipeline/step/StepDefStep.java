package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.Pipeline;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Registers a named sequence of steps for later use via {@link InvokeStep}.
 *
 * <p>Example:
 *
 * <pre>{@code
 * <stepDef id="refresh-extension-aggregate">
 *     <clear><graph>dist:supported-extensions:vocab</graph></clear>
 *     <sparqlUpdate>
 *         <sparql><![CDATA[ INSERT { ... } WHERE { ... } ]]></sparql>
 *     </sparqlUpdate>
 * </stepDef>
 * }</pre>
 *
 * The {@code <stepDef>} must appear before any {@code <invoke>} that references it.
 */
public class StepDefStep implements Step {
    private String id;
    private final List<Step> steps = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Step> getSteps() {
        return steps;
    }

    public void addStep(Step step) {
        this.steps.add(step);
    }

    @Override
    public String getElementName() {
        return "stepDef";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (id == null) {
            throw new MojoExecutionException("<stepDef> requires an id attribute");
        }
        state.registerStepDef(id, steps);
        state.log()
                .info("registered <stepDef id=\"%s\"> (%d step(s))".formatted(id, steps.size()), 1);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("stepDef".getBytes(StandardCharsets.UTF_8));
            if (id != null) {
                digest.update(id.getBytes(StandardCharsets.UTF_8));
            }
            String subPreviousHash = "";
            for (Step step : steps) {
                subPreviousHash = step.calculateHash(subPreviousHash, state);
                digest.update(subPreviousHash.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static StepDefStep parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config, "<stepDef> configuration is missing.\n" + usage());
        }
        StepDefStep step = new StepDefStep();
        String id = config.getAttribute("id");
        if (id == null || id.trim().isEmpty()) {
            throw new ConfigurationParseException(
                    config, "<stepDef> requires an 'id' attribute.\n" + usage());
        }
        step.setId(id.trim());
        for (Xpp3Dom child : config.getChildren()) {
            Step childStep =
                    Pipeline.parseStep(
                            config, child, child.getName(), "stepDef", "savepoint", "stepDef");
            step.addStep(childStep);
        }
        if (step.steps.isEmpty()) {
            throw new ConfigurationParseException(
                    config,
                    "<stepDef id=\"%s\"> requires at least one child step.\n".formatted(id)
                            + usage());
        }
        return step;
    }

    public static String usage() {
        return """
                Usage: <stepDef id="myDef"> ... one or more steps ... </stepDef>
                Register a named sequence of steps for later use with <invoke stepRef="myDef"/>.
                The <stepDef> must appear before any <invoke> that references it.
                Nesting <stepDef> or <savepoint> inside a <stepDef> is not allowed.
                Example:
                <stepDef id="refresh-extension-aggregate">
                    <clear><graph>dist:supported-extensions:vocab</graph></clear>
                    <sparqlUpdate>
                        <sparql><![CDATA[ INSERT { GRAPH <dist:supported-extensions:vocab> { ?s ?p ?o . } }
                        WHERE { GRAPH <build:supported-extensions> { ?g a <urn:qudt-build:extensions#SupportedVocabGraph> . }
                                GRAPH ?g { ?s ?p ?o . } } ]]></sparql>
                    </sparqlUpdate>
                </stepDef>""";
    }
}
