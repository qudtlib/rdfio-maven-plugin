package io.github.qudtlib.maven.rdfio.pipeline;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public class SparqlUpdateStep implements Step {
    @Parameter private String sparql;

    public String getSparql() {
        return sparql;
    }

    public void setSparql(String sparql) {
        this.sparql = sparql;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (sparql == null) {
            throw new MojoExecutionException("SPARQL query is required in sparqlUpdate step");
        }
        SparqlHelper.executeSparqlUpdateWithVariables(sparql, dataset, state.getMetadataGraph());
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("sparqlUpdate".getBytes(StandardCharsets.UTF_8));
            if (sparql != null) {
                digest.update(sparql.getBytes(StandardCharsets.UTF_8));
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
