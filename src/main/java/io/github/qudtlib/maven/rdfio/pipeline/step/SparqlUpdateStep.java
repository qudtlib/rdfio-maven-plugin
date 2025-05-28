package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class SparqlUpdateStep implements Step {
    private String sparql;

    private String file;

    public String getSparql() {
        return sparql;
    }

    public void setSparql(String sparql) {
        this.sparql = sparql;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    @Override
    public String getElementName() {
        return "sparqlUpdate";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        try {
            String sparqlString = this.sparql;
            if (sparqlString == null && this.file != null) {
                RelativePath sparqlFile =
                        state.files().make(state.variables().resolve(this.file, dataset));
                sparqlString = state.files().readText(sparqlFile);
                if (sparqlString == null || sparqlString.isBlank()) {
                    throw new MojoExecutionException(
                            "No SPARQL found in specified file %s"
                                    .formatted(sparqlFile.getRelativePath()));
                }
            }
            if (sparqlString == null) {
                throw new MojoExecutionException(
                        "SPARQL query is required in sparqlUpdate step - neither <sparql> nor <file> element had a query");
            }
            List<String> graphs = PipelineHelper.getGraphList(dataset);
            SparqlHelper.executeSparqlUpdateWithVariables(
                    sparqlString, dataset, state.getMetadataGraph());
            List<String> graphsAfterUpdate = PipelineHelper.getGraphList(dataset);
            graphsAfterUpdate.removeAll(graphs);
            graphsAfterUpdate.forEach(
                    newGraph ->
                            PipelineHelper.bindGraphToNoFileIfUnbound(dataset, state, newGraph));

            state.getPrecedingSteps().add(this);
        } catch (Exception e) {
            throw new MojoExecutionException("Error executing SparqlUpdate step", e);
        }
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
            if (file != null) {
                digest.update(file.getBytes(StandardCharsets.UTF_8));
                RdfFileProcessor.updateHashWithFiles(
                        List.of(FileHelper.resolveRelativeUnixPath(state.getBaseDir(), file)),
                        digest);
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    /**
     * Parses the XML configuration for the SPARQL Update step.
     *
     * @param config The Xpp3Dom configuration.
     * @return A configured SparqlUpdateStep instance.
     * @throws ConfigurationParseException If the configuration is invalid.
     */
    public static SparqlUpdateStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    SparqlUpdate step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        SparqlUpdateStep step = new SparqlUpdateStep();
        ParsingHelper.optionalStringChild(
                config, "sparql", step::setSparql, SparqlUpdateStep::usage);
        ParsingHelper.optionalStringChild(config, "file", step::setFile, SparqlUpdateStep::usage);
        if (step.getSparql() == null && step.getFile() == null) {
            throw new ConfigurationParseException(
                    config,
                    "Either a <sparql> or a <file> subelement must be provided\n%s"
                            .formatted(usage()));
        }
        if (step.getSparql() != null && step.getFile() != null) {
            throw new ConfigurationParseException(
                    config,
                    "Cannot have a <sparql> and a <file> subelement\n%s".formatted(usage()));
        }
        return step;
    }

    /**
     * Provides usage information for configuring the SPARQL Update step.
     *
     * @return A string describing the configuration and usage.
     */
    public static String usage() {
        return """
                Usage: Provide a <sparqlUpdate> element with either a <sparql> or a <file> subelement.
                - <sparql>: an inline sparql update
                - <file>: a file containing the query.
                Note: prefixes that are defined in the dataset do not need to be specified in the queries
                Note: SHACL SPARQL functions that have been loaded using <shaclFunctions> can be used in queries
                Note: if you have to use angled brackets ('<' and '>'), you have these options:
                    - write them as &lt; and &gt;
                    - wrap the whole query in <![CDATA[ ... (your query) ...]]>
                Example:
                <sparqlUpdate>
                    <sparql>
                        INSERT DATA { GRAPH &lt;test:graph&gt; {
                            &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt;
                        } }
                    </sparql>
                </sparqlUpdate>
                Alternative with CDATA:
                <sparqlUpdate>
                    <sparql>
                        <![CDATA[
                        INSERT DATA { GRAPH <test:graph> {
                            <http://example.org/s> <http://example.org/p> <http://example.org/o>
                        } }
                        ]]>
                    </sparql>
                </sparqlUpdate>
                Alternative with file:
                <sparqlUpdate>
                    <file>src/main/resources/update.rq</file>
                </sparqlUpdate>""";
    }
}
