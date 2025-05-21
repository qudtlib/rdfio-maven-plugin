package io.github.qudtlib.maven.rdfio.pipeline;

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

    public void setBody(Step step) {
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
                        .getIncludes()
                        .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                values.getGraphs()
                        .getExcludes()
                        .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
            }
            String subPreviousHash = "";
            for (Step subStep : body) {
                subPreviousHash = subStep.calculateHash(subPreviousHash, state);
                digest.update(subPreviousHash.getBytes(StandardCharsets.UTF_8));
            }
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    // ForeachStep.java
    public static ForeachStep parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Foreach step configuration is missing.
                            Usage: Provide a <foreach> element with <var>, <values>, and <body>.
                            Example:
                            <foreach>
                                <var>fileGraph</var>
                                <values><graphs><include>vocab:*</include></graphs></values>
                                <body><sparqlUpdate>...</sparqlUpdate></body>
                            </foreach>""");
        }

        ForeachStep step = new ForeachStep();
        Xpp3Dom varDom = config.getChild("var");
        if (varDom == null || varDom.getValue() == null || varDom.getValue().trim().isEmpty()) {
            throw new MojoExecutionException(
                    """
                            Foreach step requires a non-empty <var>.
                            Usage: Specify the variable name for iteration.
                            Example: <var>fileGraph</var>""");
        }
        step.setVar(varDom.getValue().trim());

        Xpp3Dom valuesDom = config.getChild("values");
        if (valuesDom == null) {
            throw new MojoExecutionException(
                    """
                            Foreach step requires a <values> element.
                            Usage: Specify the values to iterate over via <graphs>.
                            Example: <values><graphs><include>vocab:*</include></graphs></values>""");
        }
        step.setValues(Values.parse(valuesDom));

        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new MojoExecutionException(
                    """
                            Foreach step requires a <body> with a single step.
                            Usage: Provide a single step (e.g., <sparqlUpdate>) inside <body>.
                            Example: <body><sparqlUpdate><sparql>...</sparql></sparqlUpdate></body>""");
        }
        Xpp3Dom[] bodyChildren = bodyDom.getChildren();
        if (bodyChildren.length > 1) {
            throw new MojoExecutionException(
                    """
                            Foreach step <body> must contain exactly one step.
                            Usage: Provide a single step inside <body>.
                            Example: <body><sparqlUpdate><sparql>...</sparql></sparqlUpdate></body>""");
        }
        String bodyStepType = bodyChildren[0].getName();
        Step bodyStep =
                switch (bodyStepType) {
                    case "sparqlUpdate" -> SparqlUpdateStep.parse(bodyChildren[0]);
                    case "add" -> AddStep.parse(bodyChildren[0]);
                    case "shaclInfer" -> ShaclInferStep.parse(bodyChildren[0]);
                    case "write" -> WriteStep.parse(bodyChildren[0]);
                    case "foreach" -> ForeachStep.parse(bodyChildren[0]);
                    default ->
                            throw new MojoExecutionException(
                                    "Invalid step type in foreach <body>: "
                                            + bodyStepType
                                            + ".\n"
                                            + "Usage: Use one of: <add>, <sparqlUpdate>, <shaclInfer>, <write>, <foreach>.\n"
                                            + "Example: <body><sparqlUpdate><sparql>...</sparql></sparqlUpdate></body>");
                };
        step.setBody(bodyStep);

        return step;
    }
}
