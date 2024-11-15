package io.github.qudtlib.maven.rdfio.filter;

import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.UpdateExecution;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlUpdateFilter extends AbstractFilter {

    private final String update;

    public SparqlUpdateFilter(String update) {
        this.update = update;
    }

    public void filter(Model model) throws MojoExecutionException {
        String updateWithPrefixes = addPrefixes(update, model);
        UpdateRequest parsedUpdate;
        try {
            parsedUpdate = UpdateFactory.create(updateWithPrefixes);
        } catch (Exception e) {
            getLog().error("Cannot parse SPARQL Update: \n");
            getLog().error(updateWithPrefixes);
            getLog().error("\nProblem:");
            getLog().error(e.getMessage());
            throw new MojoExecutionException("Error parsing SPARQL Update (see error output)");
        }
        Dataset ds = DatasetFactory.createGeneral();
        ds.setDefaultModel(model);
        UpdateExecution execution = UpdateExecutionFactory.create(parsedUpdate, ds);
        execution.execute();
        model.removeAll();
        model.add(ds.getDefaultModel());
    }

    private String addPrefixes(String update, Model model) {
        String prefixes =
                model.getNsPrefixMap().entrySet().stream()
                        .map(e -> String.format("PREFIX %s: <%s>", e.getKey(), e.getValue()))
                        .collect(Collectors.joining("\n"));
        return prefixes + "\n" + update;
    }
}
