package io.github.qudtlib.maven.rdfio.filter;

import static io.github.qudtlib.maven.rdfio.filter.SparqlHelper.addPrefixes;
import static io.github.qudtlib.maven.rdfio.filter.SparqlHelper.withLineNumbers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlQueryFilter extends AbstractFilter {

    private String sparql;

    public SparqlQueryFilter(String sparql) {
        this.sparql = sparql;
    }
    ;

    public void filter(Dataset dataset) throws MojoExecutionException {
        String queryWithoutPrefixes = getSparqlSelectString();
        String sparqlWithPrefixes = addPrefixes(queryWithoutPrefixes, dataset);
        Query parsedQuery;
        try {
            parsedQuery = QueryFactory.create(sparqlWithPrefixes);
        } catch (Exception e) {
            getLog().error(
                            String.format(
                                    "Cannot parse SPARQL Select query: \n%s",
                                    withLineNumbers(sparqlWithPrefixes)));
            getLog().error(String.format("Problem:\n%s", e.getMessage()));
            throw new MojoExecutionException(
                    "Error parsing SPARQL Select query (see error output)");
        }
        if (parsedQuery.isConstructType()) {
            getLog().info(
                            "Executing SPARQL CONSTRUCT query. Results will be added to the default graph of the RDF dataset. (To change other graphs, use SparqlUpdate)");
            QueryExecution queryExec = QueryExecutionFactory.create(parsedQuery, dataset);
            Model constructedModel = queryExec.execConstruct();
            dataset.getDefaultModel().add(constructedModel);
        } else if (parsedQuery.isSelectType()) {
            getLog().info(
                            "Executing SPARQL SELECT query. Results will be logged. The RDF dataset is not changed.");
            logQueryResults(parsedQuery, dataset);
        } else if (parsedQuery.isAskType()) {
            getLog().info(
                            "Executing SPARQL ASK query. Results will be logged. The RDF dataset is not changed.");
            logQueryResults(parsedQuery, dataset);
        } else if (parsedQuery.isDescribeType()) {
            getLog().info(
                            "Executing SPARQL DESCRIBE query. Results will be logged. The RDF dataset is not changed.");
            logQueryResults(parsedQuery, dataset);
        } else {
            throw new MojoExecutionException(
                    String.format(
                            "Provided query is not a SELECT, CONSTRUCT, ASK or DESCRIBE query: %s",
                            queryWithoutPrefixes));
        }
    }

    public void logQueryResults(Query parsedQuery, Dataset dataset) throws MojoExecutionException {
        try (QueryExecution queryExec = QueryExecutionFactory.create(parsedQuery, dataset)) {

            OutputStream outputStream = new ByteArrayOutputStream();

            if (parsedQuery.isSelectType()) {
                // Format the ResultSet into the OutputStream
                ResultSetFormatter.out(outputStream, queryExec.execSelect());
            } else if (parsedQuery.isDescribeType()) {
                queryExec.execDescribe().write(outputStream, "TTL");
            } else if (parsedQuery.isAskType()) {
                boolean result = queryExec.execAsk();
                outputStream.write(
                        ("   Result of ASK query: " + result).getBytes(StandardCharsets.UTF_8));
            } else {
                throw new IllegalArgumentException(
                        "Cannot execute SPARQL query - unknown query type");
            }

            // Convert the output to a string
            String results = outputStream.toString();

            // Log the table with a prefix for clarity
            getLog().info("Query results:\n" + results);
        } catch (IOException e) {
            throw new MojoExecutionException(e);
        }
    }

    protected String getSparqlSelectString() throws MojoExecutionException {
        return this.sparql;
    }
}
