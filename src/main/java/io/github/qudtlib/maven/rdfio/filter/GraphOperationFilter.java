package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class GraphOperationFilter extends AbstractMultiGraphFilter
        implements OutputToGraphFilter {
    @Parameter private boolean clearBeforeInsert = false;
    @Parameter private String outputGraph;

    public void filter(Dataset dataset) throws MojoExecutionException {
        Model result = operation(dataset);

        if (GraphsHelper.hasNamedGraph(dataset, outputGraph)) {
            Model target = GraphsHelper.getModel(dataset, outputGraph);
            if (isClearBeforeInsert()) {
                target.removeAll();
            }
            target.add(result);
        } else {
            dataset.addNamedModel(GraphOperationFilter.this.getOutputGraph(), result);
        }
    }

    protected abstract Model operation(Dataset dataset);

    @Override
    public String getOutputGraph() {
        return "";
    }

    @Override
    public boolean isClearBeforeInsert() {
        return false;
    }
}
