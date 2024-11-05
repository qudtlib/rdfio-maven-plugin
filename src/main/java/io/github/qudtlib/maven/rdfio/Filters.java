package io.github.qudtlib.maven.rdfio;

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugins.annotations.Parameter;

public class Filters {
    private List<Filter> filters = new ArrayList<>();

    @Parameter
    public void setInclude(IncludeFilter includeFilter) {
        filters.add(includeFilter);
    }

    @Parameter
    public void setExclude(ExcludeFilter excludeFilter) {
        filters.add(excludeFilter);
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void filter(Model inputGraph) {
        filters.forEach(filter -> filter.filter(inputGraph));
    }
}
