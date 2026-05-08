package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
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
import java.util.Map;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Iterates over a list of {@code <env>} configurations, executing a shared {@code <body>} once per
 * environment.
 *
 * <p>Before the loop, all pipeline variable bindings are saved. For each iteration, the
 * corresponding {@code <env>} variables are injected into the metadata graph so that the {@code
 * <body>} steps resolve them via the standard {@code ${varName}} mechanism. After each iteration,
 * the injected variables are removed. After the loop, all bindings are restored to their pre-loop
 * state.
 *
 * <p>The optional {@code id} attribute on each {@code <env>} is used only for log messages and is
 * not automatically injected as a variable — add an explicit {@code <property name="id">} if the
 * body needs it.
 *
 * <p>Example:
 *
 * <pre>{@code
 * <forEachEnv>
 *     <env id="symbol">
 *         <property name="name">symbol</property>
 *         <property name="property">qudt:symbol</property>
 *     </env>
 *     <env id="ucumCode">
 *         <property name="name">ucumCode</property>
 *         <property name="property">qudt:ucumCode</property>
 *     </env>
 *     <body>
 *         <sparqlUpdate>
 *             <sparql><![CDATA[
 *                 INSERT { GRAPH <result:${name}> { ?s ${property} ?o } }
 *                 WHERE  { ?s ${property} ?o }
 *             ]]></sparql>
 *         </sparqlUpdate>
 *     </body>
 * </forEachEnv>
 * }</pre>
 */
public class ForEachEnvStep implements Step {

    private String message;
    private final List<EnvStep> envs = new ArrayList<>();
    private final List<Step> body = new ArrayList<>();

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<EnvStep> getEnvs() {
        return envs;
    }

    public void addEnv(EnvStep env) {
        this.envs.add(env);
    }

    public List<Step> getBody() {
        return body;
    }

    public void addBodyStep(Step step) {
        this.body.add(step);
    }

    @Override
    public String getElementName() {
        return "forEachEnv";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (envs.isEmpty()) {
            throw new MojoExecutionException("<forEachEnv> requires at least one <env>");
        }
        if (body.isEmpty()) {
            throw new MojoExecutionException("<forEachEnv> requires a non-empty <body>");
        }
        if (message != null) {
            state.log().info(message, 1);
        }
        List<Statement> snapshot = snapshotVariables(dataset, state.getMetadataGraph());
        state.incIndentLevel();
        try {
            for (EnvStep env : envs) {
                String envId = env.getProperties().getOrDefault("id", "(unnamed)");
                state.log().info("<forEachEnv> env: " + envId);
                for (Map.Entry<String, String> entry : env.getProperties().entrySet()) {
                    PipelineHelper.setPipelineVariable(
                            dataset,
                            state,
                            entry.getKey(),
                            ResourceFactory.createStringLiteral(entry.getValue()));
                }
                try {
                    for (Step step : body) {
                        step.executeAndWrapException(dataset, state);
                    }
                } finally {
                    clearVariables(dataset, state.getMetadataGraph(), env.getProperties());
                }
            }
        } finally {
            state.decIndentLevel();
            restoreVariables(dataset, state.getMetadataGraph(), snapshot);
        }
        state.getPrecedingSteps().add(this);
    }

    private static List<Statement> snapshotVariables(Dataset dataset, String metadataGraph) {
        Model metaModel = dataset.getNamedModel(metadataGraph);
        return metaModel.listStatements(null, RDFIO.value, (RDFNode) null).toList().stream()
                .filter(
                        s ->
                                s.getSubject().isURIResource()
                                        && s.getSubject()
                                                .getURI()
                                                .startsWith(RDFIO.VARIABLE_PREFIX))
                .toList();
    }

    private static void clearVariables(
            Dataset dataset, String metadataGraph, Map<String, String> properties) {
        Model metaModel = dataset.getNamedModel(metadataGraph);
        for (String name : properties.keySet()) {
            metaModel.removeAll(RDFIO.makeVariableUri(name), RDFIO.value, null);
        }
    }

    private static void restoreVariables(
            Dataset dataset, String metadataGraph, List<Statement> snapshot) {
        Model metaModel = dataset.getNamedModel(metadataGraph);
        metaModel.listStatements(null, RDFIO.value, (RDFNode) null).toList().stream()
                .filter(
                        s ->
                                s.getSubject().isURIResource()
                                        && s.getSubject()
                                                .getURI()
                                                .startsWith(RDFIO.VARIABLE_PREFIX))
                .forEach(metaModel::remove);
        snapshot.forEach(metaModel::add);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("forEachEnv".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            for (EnvStep env : envs) {
                for (Map.Entry<String, String> entry : env.getProperties().entrySet()) {
                    digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
                    digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
                }
            }
            String subHash = "";
            for (Step step : body) {
                subHash = step.calculateHash(subHash, state);
                digest.update(subHash.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static ForEachEnvStep parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config, "<forEachEnv> configuration is missing.\n" + usage());
        }
        ForEachEnvStep step = new ForEachEnvStep();
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, ForEachEnvStep::usage);

        for (Xpp3Dom child : config.getChildren()) {
            switch (child.getName()) {
                case "env" -> step.addEnv(EnvStep.parse(child));
                case "body" -> {
                    for (Xpp3Dom bodyStepDom : child.getChildren()) {
                        step.addBodyStep(
                                Pipeline.parseStep(
                                        config,
                                        bodyStepDom,
                                        bodyStepDom.getName(),
                                        "forEachEnv",
                                        "savepoint",
                                        "stepDef"));
                    }
                }
                case "message" -> {} // already handled by ParsingHelper above
                default ->
                        throw new ConfigurationParseException(
                                child,
                                "Unexpected element <%s> inside <forEachEnv>. Expected <env> or <body>.\n%s"
                                        .formatted(child.getName(), usage()));
            }
        }

        if (step.envs.isEmpty()) {
            throw new ConfigurationParseException(
                    config, "<forEachEnv> requires at least one <env>.\n" + usage());
        }
        if (step.body.isEmpty()) {
            throw new ConfigurationParseException(
                    config, "<forEachEnv> requires a <body> with at least one step.\n" + usage());
        }
        return step;
    }

    public static String usage() {
        return """
                Usage: <forEachEnv> with one or more <env> children and a shared <body>.
                Each <env> may have <property name="propName">value</property> entries.
                Within <body>, ${propName} is resolved from the current env's variables.
                The optional id attribute on <env> is for logging only; to use it in the
                body, add an explicit <property name="id">...</property>.
                <savepoint> and <stepDef> are not allowed inside <forEachEnv>.
                Example:
                <forEachEnv>
                    <env id="core">
                        <property name="suffix"></property>
                    </env>
                    <env id="extensions">
                        <property name="suffix">-ext</property>
                    </env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>""";
    }
}
