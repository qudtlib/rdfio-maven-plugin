package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;

public abstract class AbstractMultiGraphPerGraphFilter extends AbstractMultiGraphFilter {

    public void filter(Dataset dataset) throws MojoExecutionException {
        for (Model model : GraphsHelper.getModels(dataset, getGraphs())) {
            filterModel(model);
        }
    }

    protected abstract void filterModel(Model model) throws MojoExecutionException;
}
