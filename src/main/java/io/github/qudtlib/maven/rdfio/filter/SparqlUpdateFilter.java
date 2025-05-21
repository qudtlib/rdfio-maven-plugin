package io.github.qudtlib.maven.rdfio.filter;

import static io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.addPrefixes;
import static io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.withLineNumbers;

import org.apache.jena.query.Dataset;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlUpdateFilter extends AbstractFilter {

    protected String sparql;

    public SparqlUpdateFilter(String sparql) {
        this.sparql = sparql;
    }

    public void filter(Dataset dataset) throws MojoExecutionException {
        String updateWithoutPrefixes = getSparqlUpdateString();
        String updateWithPrefixes = addPrefixes(updateWithoutPrefixes, dataset);
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
        UpdateAction.execute(parsedUpdate, dataset);
    }

    protected String getSparqlUpdateString() throws MojoExecutionException {
        return this.sparql;
    }
}
