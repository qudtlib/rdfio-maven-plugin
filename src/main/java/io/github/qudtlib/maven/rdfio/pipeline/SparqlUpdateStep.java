package io.github.qudtlib.maven.rdfio.pipeline;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class SparqlUpdateStep implements Step {
    private String sparql;

    public String getSparql() {
        return sparql;
    }

    public void setSparql(String sparql) {
        this.sparql = sparql;
    }

    @Override
    public String getElementName() {
        return "sparqlUpdate";
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
    public String calculateHash(String previousHash, PipelineState state) {
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

    // SparqlUpdateStep.java
    public static SparqlUpdateStep parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            SparqlUpdate step configuration is missing.
                            Usage: Provide a <sparqlUpdate> element with a <sparql> query.
                            Example:
                            <sparqlUpdate>
                                <sparql>INSERT DATA { GRAPH &lt;test:graph&gt; { ... } }</sparql>
                            </sparqlUpdate>""");
        }

        SparqlUpdateStep step = new SparqlUpdateStep();
        Xpp3Dom sparqlDom = config.getChild("sparql");
        if (sparqlDom == null
                || sparqlDom.getValue() == null
                || sparqlDom.getValue().trim().isEmpty()) {
            throw new MojoExecutionException(
                    """
                            SparqlUpdate step requires a non-empty <sparql> query.
                            Usage: Provide a valid SPARQL Update query.
                            Example: <sparql>INSERT DATA { GRAPH &lt;test:graph&gt; { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }</sparql>""");
        }
        step.setSparql(sparqlDom.getValue().trim());
        return step;
    }
}
