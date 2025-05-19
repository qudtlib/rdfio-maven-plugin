package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class GraphUnionFilter extends GraphOperationFilter {
    @Override
    protected Model operation(Dataset dataset) {
        Model result = ModelFactory.createDefaultModel();
        for (Model model : GraphsHelper.getModels(dataset, getGraphs())) {
            result.add(model);
        }
        return result;
    }
}
