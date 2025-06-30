package io.github.qudtlib.maven.rdfio.pipeline.step;

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
import org.apache.jena.rdf.model.*;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ForeachStep implements Step {
    private String message;

    private String var;

    private String indexVar;

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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getIndexVar() {
        return indexVar;
    }

    public void setIndexVar(String indexVar) {
        this.indexVar = indexVar;
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
        if (message != null) {
            state.log().info(message, 1);
        }
        state.log().info("loop variable: " + var, 1);
        if (indexVar != null) {
            state.log().info("index variable: " + indexVar, 1);
        }
        List<RDFNode> graphNames = values.getValues(dataset, state);
        try {
            int index = 0;
            state.incIndentLevel();
            for (RDFNode currentValue : graphNames) {
                index++;
                state.log().info("<foreach> iteration %d: entering body".formatted(index));
                PipelineHelper.setPipelineVariable(dataset, state, var, currentValue);
                state.log().info("variables", 1);
                state.log().info("%s=%s".formatted(this.var, currentValue), 2);
                if (this.indexVar != null) {
                    state.log().info("%s=%s".formatted(this.indexVar, index), 2);
                }
                state.log().info("");
                PipelineHelper.setPipelineVariable(
                        dataset,
                        state,
                        indexVar,
                        ResourceFactory.createTypedLiteral(Integer.toString(index)));
                for (Step step : body) {
                    step.executeAndWrapException(dataset, state);
                }
                state.log().info("<foreach> iteration %d: end of body".formatted(index));
            }
        } finally {
            state.decIndentLevel();
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
            if (values != null) {
                values.updateHash(digest, state);
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
                    config,
                    """
                            Foreach step configuration is missing.
                            %s"""
                            .formatted(step.usage()));
        }

        ParsingHelper.requiredStringChild(config, "var", step::setVar, step::usage);
        ParsingHelper.optionalStringChild(config, "indexVar", step::setIndexVar, step::usage);
        ParsingHelper.optionalStringChild(config, "message", step::setMessage, step::usage);
        ParsingHelper.requiredDomChild(
                config, "values", Values::parse, step::setValues, step::usage);

        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Foreach step requires a <body> with a single step.
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
                                        "Invalid step type in foreach <body>: "
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
