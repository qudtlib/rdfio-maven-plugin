package io.github.qudtlib.maven.rdfio.filter;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlSelectFilter extends AbstractFilter {

    protected final String selectQuery;

    public SparqlSelectFilter(String constructQuery) {
        this.selectQuery = constructQuery;
    }

    public void filter(Model model) throws MojoExecutionException {
        String constructWithPrefixes = addPrefixes(getSparqlSelectString(), model);
        Query parsedQuery;
        try {
            parsedQuery = QueryFactory.create(constructWithPrefixes);
        } catch (Exception e) {
            getLog().error(
                            String.format(
                                    "Cannot parse SPARQL Select query: \n%s",
                                    withLineNumbers(constructWithPrefixes)));
            getLog().error(String.format("Problem:\n%s", e.getMessage()));
            throw new MojoExecutionException(
                    "Error parsing SPARQL Select query (see error output)");
        }
        if (!parsedQuery.isSelectType()) {
            throw new MojoExecutionException(
                    String.format("Provided query is not a Select query: %s", selectQuery));
        }
        logQueryResults(parsedQuery, model);
    }

    public void logQueryResults(Query parsedQuery, Model model) {
        QueryExecution queryExec = null;
        try {
            queryExec = QueryExecutionFactory.create(parsedQuery, model);
            OutputStream outputStream = new ByteArrayOutputStream();

            // Format the ResultSet into the OutputStream
            ResultSetFormatter.out(outputStream, queryExec.execSelect());

            // Convert the output to a string
            String results = outputStream.toString();

            // Log the table with a prefix for clarity
            getLog().info("Query results:\n" + results);
        } finally {
            // Ensure the QueryExecution is closed to free resources
            queryExec.close();
        }
    }

    protected String getSparqlSelectString() throws MojoExecutionException {
        return this.selectQuery;
    }

    private String withLineNumbers(String updateWithPrefixes) {
        String[] lines = updateWithPrefixes.split("\n");
        int width = String.valueOf(lines.length).length();
        return IntStream.range(0, lines.length)
                .mapToObj(i -> String.format("%" + width + "d %s", i + 1, lines[i]))
                .collect(Collectors.joining("\n"));
    }

    private String addPrefixes(String update, Model model) {
        String prefixes =
                model.getNsPrefixMap().entrySet().stream()
                        .map(e -> String.format("PREFIX %s: <%s>", e.getKey(), e.getValue()))
                        .collect(Collectors.joining("\n"));
        return prefixes + "\n" + update;
    }
}
