package io.github.qudtlib.maven.rdfio.filter;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.UpdateExecution;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlUpdateFilter extends AbstractFilter {

    protected final String update;

    public SparqlUpdateFilter(String update) {
        this.update = update;
    }

    public void filter(Model model) throws MojoExecutionException {
        String updateWithPrefixes = addPrefixes(getSparqlUpdateString(), model);
        UpdateRequest parsedUpdate;
        try {
            parsedUpdate = UpdateFactory.create(updateWithPrefixes);
        } catch (Exception e) {
            getLog().error(
                            String.format(
                                    "Cannot parse SPARQL Update: \n%s",
                                    withLineNumbers(updateWithPrefixes)));
            getLog().error(String.format("Problem:\n%s", e.getMessage()));
            throw new MojoExecutionException("Error parsing SPARQL Update (see error output)");
        }
        Dataset ds = DatasetFactory.createGeneral();
        ds.setDefaultModel(model);
        UpdateExecution execution = UpdateExecutionFactory.create(parsedUpdate, ds);
        execution.execute();
        model.removeAll();
        model.add(ds.getDefaultModel());
    }

    protected String getSparqlUpdateString() throws MojoExecutionException {
        return this.update;
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
