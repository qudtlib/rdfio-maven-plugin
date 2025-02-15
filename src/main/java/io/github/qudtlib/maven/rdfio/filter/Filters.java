package io.github.qudtlib.maven.rdfio.filter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;

public class Filters {
    private final List<Filter> filters = new ArrayList<>();
    private Log log;

    @Parameter(defaultValue = "${project.basedir}", readonly = true)
    protected File basedir;

    @Parameter
    public void setInclude(IncludeFilter includeFilter) {
        filters.add(includeFilter);
    }

    @Parameter
    public void setExclude(ExcludeFilter excludeFilter) {
        filters.add(excludeFilter);
    }

    @Parameter
    public void setSparqlUpdate(String update) {
        filters.add(new SparqlUpdateFilter(update));
    }

    @Parameter
    public void setSparqlUpdateFile(String filename) {
        try {
            filters.add(new SparqlUpdateFileFilter(filename, basedir));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Error loading sparql update from file %s", filename), e);
        }
    }

    @Parameter
    public void setSparqlConstruct(String construct) {
        filters.add(new SparqlConstructFilter(construct));
    }

    @Parameter
    public void setSparqlConstructFile(String filename) {
        try {
            filters.add(new SparqlConstructFileFilter(filename, basedir));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Error loading sparql construct query from file %s", filename),
                    e);
        }
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void filter(Model inputGraph) throws MojoExecutionException {
        for (Filter filter : this.filters) {
            long sizeBefore = inputGraph.size();
            long start = System.currentTimeMillis();
            filter.setLog(log);
            filter.filter(inputGraph);
            long sizeAfter = inputGraph.size();
            long end = System.currentTimeMillis();
            log.info(
                    String.format(
                            "Filter %s changed statement count from %d to %d (diff: %d) in %d millis",
                            filter.getClass().getSimpleName(),
                            sizeBefore,
                            sizeAfter,
                            sizeAfter - sizeBefore,
                            end - start));
        }
    }

    public void setLog(Log log) {
        this.log = log;
    }
}
