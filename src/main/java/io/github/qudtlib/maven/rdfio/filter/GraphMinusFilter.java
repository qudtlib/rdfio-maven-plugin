package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;

public class GraphMinusFilter extends GraphOperationFilter {

    @Override
    protected Model operation(Dataset dataset) {
        Model result = null;
        for (Model model : GraphsHelper.getModels(dataset, getGraphs())) {
            if (result == null) {
                result = model;
            } else {
                result = result.remove(model);
            }
        }
        return result;
    }
}
