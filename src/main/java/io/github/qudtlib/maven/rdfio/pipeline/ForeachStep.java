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
import org.apache.maven.plugins.annotations.Parameter;

public class ForeachStep implements Step {
    @Parameter private String var;

    @Parameter private Values values;

    @Parameter private List<Step> body = new ArrayList<>();

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
                subPreviousHash = step.calculateHash(subPreviousHash);
                step.execute(dataset, state);
            }
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash) {
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
                subPreviousHash = subStep.calculateHash(subPreviousHash);
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
}
