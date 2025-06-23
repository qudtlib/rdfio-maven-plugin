package io.github.qudtlib.maven.rdfio.pipeline.step;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.AsciiTableBuilder;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class SparqlQueryStep implements Step {
    private String sparql;

    private String file;

    private String message;

    private String toFile;

    private RelativePath toFilePath = null;

    private String toGraph;

    private String toGraphName;

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

    public String getToGraph() {
        return toGraph;
    }

    public void setToGraph(String toGraph) {
        this.toGraph = toGraph;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getToFile() {
        return toFile;
    }

    public void setToFile(String toFile) {
        this.toFile = toFile;
    }

    @Override
    public String getElementName() {
        return "sparqlQuery";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        try {
            if (message != null) {
                state.log().info(message, 1);
            }
            if (toFile != null) {
                toFilePath = state.files().make(state.variables().resolve(this.toFile, dataset));
            }
            if (toGraph != null) {
                toGraphName = state.variables().resolve(this.toGraph, dataset);
            }
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
                        "SPARQL query is required in sparqlQuery step - neither <sparql> nor <file> element had a query");
            }
            SparqlHelper.QueryResultProcessor resultProcessor =
                    new SparqlHelper.QueryResultProcessor() {
                        @Override
                        public void processAskResult(boolean result) {
                            state.log().info("Result of SPARQL ASK query: " + result, 1);
                            if (toFile != null) {
                                state.log()
                                        .info(
                                                "toFile parameter is present but ignored for SPARQL ASK queries",
                                                1);
                            }
                            if (toGraph != null) {
                                state.log()
                                        .info(
                                                "toGraph parameter is present but ignored for SPARQL ASK queries",
                                                1);
                            }
                        }

                        @Override
                        public void processSelectResult(ResultSet result) {
                            String table = ResultSetFormatter.asText(result);
                            List<String> lines = Arrays.stream(table.split("\n")).toList();

                            if (toFile == null) {
                                state.log().info("SPARQL SELECT result: ", 1);
                                state.log().info(lines, 2);
                                state.log()
                                        .info(
                                                "Use 'toFile' parameter to write this result to a file instead.",
                                                1);
                            } else {
                                state.log()
                                        .info(
                                                "Writing SPARQL SELECT result to file %s"
                                                        .formatted(toFilePath.getRelativePath()),
                                                1);
                                state.files().writeText(toFilePath, table);
                            }
                            if (toGraph != null) {
                                state.log()
                                        .info(
                                                "toGraph parameter is present but ignored for SPARQL SELECT queries",
                                                1);
                            }
                        }

                        @Override
                        public void processConstructOrDescribeResult(Model result) {
                            if (toFile == null && toGraph == null) {
                                AsciiTableBuilder tableBuilder = AsciiTable.builder();
                                tableBuilder.header(
                                        new String[] {"Subject", "Predicate", "Object"});
                                StmtIterator it = result.listStatements();
                                List<List<RDFNode>> tableData = new ArrayList<>();
                                while (it.hasNext()) {
                                    Statement s = it.nextStatement();
                                    tableData.add(
                                            List.of(
                                                    s.getSubject(),
                                                    s.getPredicate(),
                                                    s.getObject()));
                                }
                                RDFNode[][] dataAsArray =
                                        tableData.stream()
                                                .map(row -> row.toArray(RDFNode[]::new))
                                                .toArray(RDFNode[][]::new);
                                tableBuilder.data(dataAsArray);
                                String table = tableBuilder.lineSeparator("\n").asString();
                                List<String> lines = Arrays.stream(table.split("\n")).toList();
                                state.log().info("SPARQL CONSTRUCT/DESCRIBE result: ", 1);
                                state.log().info(lines, 2);
                                state.log()
                                        .info(
                                                "Use 'toFile' parameter to write this result to a file instead, 'toGraph', to write to a graph",
                                                1);
                            }
                            if (toGraph != null) {
                                state.log()
                                        .info(
                                                "Writing SPARQL CONSTRUCT/DESCRIBE result to graph %s"
                                                        .formatted(toGraphName),
                                                1);
                                PipelineHelper.addDataToGraph(dataset, state, result, toGraphName);
                            }
                            if (toFile != null) {
                                state.log()
                                        .info(
                                                "Writing SPARQL CONSTRUCT/DESCRIBE result to file %s"
                                                        .formatted(toFilePath));
                                state.files().writeRdf(toFilePath, result);
                            }
                        }
                    };
            SparqlHelper.executeSparqlQueryWithVariables(
                    sparqlString, dataset, state.getMetadataGraph(), resultProcessor);
            state.getPrecedingSteps().add(this);
        } catch (Exception e) {
            throw new MojoExecutionException("Error executing SparqlQuery step", e);
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("sparqlQuery".getBytes(StandardCharsets.UTF_8));
            if (sparql != null) {
                digest.update(sparql.getBytes(StandardCharsets.UTF_8));
            }
            if (file != null) {
                digest.update(file.getBytes(StandardCharsets.UTF_8));
                RdfFileProcessor.updateHashWithFiles(
                        List.of(FileHelper.resolveRelativeUnixPath(state.getBaseDir(), file)),
                        digest);
            }
            if (toFile != null) {
                digest.update(toFile.getBytes(StandardCharsets.UTF_8));
            }
            if (toGraph != null) {
                digest.update(toGraph.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    /**
     * Parses the XML configuration for the SPARQL Query step.
     *
     * @param config The Xpp3Dom configuration.
     * @return A configured SparqlQueryStep instance.
     * @throws ConfigurationParseException If the configuration is invalid.
     */
    public static SparqlQueryStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    SparqlQuery step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        SparqlQueryStep step = new SparqlQueryStep();
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, SparqlQueryStep::usage);
        ParsingHelper.optionalStringChild(
                config, "sparql", step::setSparql, SparqlQueryStep::usage);
        ParsingHelper.optionalStringChild(config, "file", step::setFile, SparqlQueryStep::usage);
        ParsingHelper.optionalStringChild(
                config, "toFile", step::setToFile, SparqlQueryStep::usage);
        ParsingHelper.optionalStringChild(
                config, "toGraph", step::setToGraph, SparqlQueryStep::usage);
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
     * Provides usage information for configuring the SPARQL Query step.
     *
     * @return A string describing the configuration and usage.
     */
    public static String usage() {
        return """
                Usage: Provide a <sparqlQuery> element with either a <sparql> or a <file> subelement.
                - <sparql>: an inline sparql query (ASK/SELECT/DESCRIBE/CONSTRUCT)
                - <file>: a file containing the query.
                - <toFile>: write the result to the specified file
                - <toGraph>: write the result to the specified graph
                Note: prefixes that are defined in the dataset do not need to be specified in the queries
                Note: SHACL SPARQL functions that have been loaded using <shaclFunctions> can be used in queries
                Note: if you have to use angled brackets ('<' and '>'), you have these options:
                    - write them as &lt; and &gt;
                    - wrap the whole query in <![CDATA[ ... (your query) ...]]>
                Example:
                <sparqlQuery>
                    <sparql>
                        CONSTRUCT {
                            &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt;
                        } WHERE {}
                    </sparql>
                </sparqlQuery>
                Alternative with CDATA:
                <sparqlQuery>
                    <sparql>
                        <![CDATA[
                        CONSTRUCT {
                            <http://example.org/s> <http://example.org/p> <http://example.org/o>
                        } WHERE {}
                        ]]>
                    </sparql>
                </sparqlQuery>
                Alternative with file:
                <sparqlQuery>
                    <file>src/main/resources/query.rq</file>
                </sparqlQuery>""";
    }
}
