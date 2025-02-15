package io.github.qudtlib.maven.rdfio.filter;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlConstructFilter extends AbstractFilter {

    protected final String constructQuery;

    public SparqlConstructFilter(String constructQuery) {
        this.constructQuery = constructQuery;
    }

    public void filter(Model model) throws MojoExecutionException {
        String constructWithPrefixes = addPrefixes(getSparqlConstructString(), model);
        Query parsedQuery;
        try {
            parsedQuery = QueryFactory.create(constructWithPrefixes);
        } catch (Exception e) {
            getLog().error(
                            String.format(
                                    "Cannot parse SPARQL Construct query: \n%s",
                                    withLineNumbers(constructWithPrefixes)));
            getLog().error(String.format("Problem:\n%s", e.getMessage()));
            throw new MojoExecutionException(
                    "Error parsing SPARQL Construct query (see error output)");
        }
        if (!parsedQuery.isConstructType()) {
            throw new MojoExecutionException(
                    String.format("Provided query is not a Construct query: %s", constructQuery));
        }
        QueryExecution queryExec = QueryExecutionFactory.create(parsedQuery, model);
        Model constructedModel = queryExec.execConstruct();
        model.add(constructedModel);
    }

    protected String getSparqlConstructString() throws MojoExecutionException {
        return this.constructQuery;
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
