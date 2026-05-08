package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Sets one or more named pipeline variables in the metadata graph.
 *
 * <p>Bindings persist for the remainder of the pipeline (or until overridden). Within a {@code
 * <forEachEnv>} loop, bindings set by iteration-level {@code <env>} elements are restored to their
 * pre-loop state when the loop finishes.
 *
 * <p>Example:
 *
 * <pre>{@code
 * <env>
 *     <property name="suffix">-ext</property>
 *     <property name="dataGraph">dist:vocab:VOCAB_QUDT-UNITS-ALL.ttl</property>
 * </env>
 * }</pre>
 */
public class EnvStep implements Step {

    private final Map<String, String> properties = new LinkedHashMap<>();

    public Map<String, String> getProperties() {
        return properties;
    }

    public void addProperty(String name, String value) {
        this.properties.put(name, value);
    }

    @Override
    public String getElementName() {
        return "env";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PipelineHelper.setPipelineVariable(
                    dataset,
                    state,
                    entry.getKey(),
                    ResourceFactory.createStringLiteral(entry.getValue()));
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("env".getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
                digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static EnvStep parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config, "<env> configuration is missing.\n" + usage());
        }
        EnvStep step = new EnvStep();
        for (Xpp3Dom child : config.getChildren()) {
            if ("property".equals(child.getName())) {
                String name = child.getAttribute("name");
                if (name == null || name.trim().isEmpty()) {
                    throw new ConfigurationParseException(
                            child, "<property> requires a 'name' attribute.\n" + usage());
                }
                String value = child.getValue() != null ? child.getValue().trim() : "";
                step.addProperty(name.trim(), value);
            } else {
                throw new ConfigurationParseException(
                        child,
                        "Unexpected element <%s> inside <env>. Expected <property>.\n%s"
                                .formatted(child.getName(), usage()));
            }
        }
        return step;
    }

    public static String usage() {
        return """
                Usage: <env> with <property name="...">value</property> children.
                Sets pipeline variable bindings in the metadata graph.
                Example:
                <env>
                    <property name="suffix">-ext</property>
                    <property name="dataGraph">dist:vocab:VOCAB_QUDT-UNITS-ALL.ttl</property>
                </env>""";
    }
}
