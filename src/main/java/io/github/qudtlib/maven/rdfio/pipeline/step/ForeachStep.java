package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.pipeline.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.Values;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ForeachStep implements Step {
    private String var;

    private Values values;

    private final List<Step> body = new ArrayList<>();

    public String getVar() {
        return var;
    }

    public void setVar(String var) {
        this.var = var;
    }

    public Values getValues() {
        return values;
    }

    public void setValues(Values values) {
        this.values = values;
    }

    public List<Step> getBody() {
        return body;
    }

    public void addBodyStep(Step step) {
        this.body.add(step);
    }

    @Override
    public String getElementName() {
        return "foreach";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (var == null || values == null || body.isEmpty()) {
            throw new MojoExecutionException("Var, values, and body are required in foreach step");
        }
        List<String> graphNames = PipelineHelper.getGraphs(dataset, values.getGraphs());
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        Resource varRes = metaModel.createResource(RDFIO.VARIABLE_PREFIX + var);
        Property valueProp = RDFIO.value;
        String previousHash = state.getPreviousStepHash();
        for (String graphName : graphNames) {
            metaModel.removeAll(varRes, valueProp, null);
            metaModel.add(varRes, valueProp, ResourceFactory.createResource(graphName));
            String subPreviousHash = previousHash;
            for (Step step : body) {
                subPreviousHash = step.calculateHash(subPreviousHash, state);
                step.execute(dataset, state);
            }
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("foreach".getBytes(StandardCharsets.UTF_8));
            if (var != null) {
                digest.update(var.getBytes(StandardCharsets.UTF_8));
            }
            if (values != null && values.getGraphs() != null) {
                values.getGraphs()
                        .getInclude()
                        .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                values.getGraphs()
                        .getExclude()
                        .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
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

    // ForeachStep.java
    public static ForeachStep parse(Xpp3Dom config) {
        ForeachStep step = new ForeachStep();
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            Foreach step configuration is missing.
                            %s"""
                            .formatted(step.usage()));
        }

        ParsingHelper.requiredStringChild(config, "var", step::setVar, step::usage);
        ParsingHelper.requiredDomChild(
                config, "values", Values::parse, step::setValues, step::usage);

        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    """
                            Foreach step requires a <body> with a single step.
                            %s"""
                            .formatted(step.usage()));
        }
        for (Xpp3Dom bodyStepConfig : bodyDom.getChildren()) {
            String bodyStepType = bodyStepConfig.getName();
            Step bodyStep =
                    switch (bodyStepType) {
                        case "sparqlUpdate" -> SparqlUpdateStep.parse(bodyStepConfig);
                        case "add" -> AddStep.parse(bodyStepConfig);
                        case "shaclInfer" -> ShaclInferStep.parse(bodyStepConfig);
                        case "write" -> WriteStep.parse(bodyStepConfig);
                        case "foreach" -> ForeachStep.parse(bodyStepConfig);
                        default ->
                                throw new ConfigurationParseException(
                                        "Invalid step type in foreach <body>: "
                                                + bodyStepType
                                                + ".\n"
                                                + "Usage: Use one of: <add>, <sparqlUpdate>, <shaclInfer>, <write>, <foreach>.\n"
                                                + "Example: <body><sparqlUpdate><sparql>...</sparql></sparqlUpdate></body>");
                    };
            step.addBodyStep(bodyStep);
        }

        return step;
    }

    public String usage() {
        return """
                            Foreach step configuration is missing.
                            Usage: Provide a <foreach> element with <var>, <values>, and <body>.
                            Example:
                            <foreach>
                                <var>fileGraph</var>
                                <values><graphs><include>vocab:*</include></graphs></values>
                                <body><sparqlUpdate>...</sparqlUpdate></body>
                            </foreach>""";
    }
}
