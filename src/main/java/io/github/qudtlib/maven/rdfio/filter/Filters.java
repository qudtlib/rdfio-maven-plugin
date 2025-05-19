package io.github.qudtlib.maven.rdfio.filter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;

public class Filters {
    private final List<Filter> filters = new ArrayList<>();
    private Log log;

    @Parameter(defaultValue = "${project.basedir}", readonly = true)
    protected File basedir;

    @Parameter
    public void setGraphUnion(GraphUnionFilter unionFilter) {
        filters.add(unionFilter);
    }

    @Parameter
    void setGraphMinus(GraphMinusFilter minusFilter) {
        filters.add(minusFilter);
    }

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
    public void setSparqlSelect(String select) {
        filters.add(new SparqlQueryFilter(select));
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
    public void setSparqlSelectFile(String filename) {
        try {
            filters.add(new SparqlQueryFileFilter(filename, basedir));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Error loading sparql select from file %s", filename), e);
        }
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void filter(Dataset dataset) throws MojoExecutionException {
        for (Filter filter : this.filters) {
            Map<String, Long> sizesBefore = GraphsHelper.getGraphSizes(dataset);
            long start = System.currentTimeMillis();
            filter.setLog(log);
            filter.filter(dataset);
            long end = System.currentTimeMillis();
            Map<String, Long> sizesAfter = GraphsHelper.getGraphSizes(dataset);
            if (sizesAfter.size() > 1) {
                String message =
                        String.format(
                                """
                Filter %s changed per-graph statement counts in %d millis as follows:
                %s
                %s
                """,
                                filter.getClass().getSimpleName(),
                                end - start,
                                sizesBefore.entrySet().stream()
                                        .map(
                                                e -> {
                                                    String key = e.getKey();
                                                    long before = e.getValue();
                                                    long after = sizesAfter.get(key);
                                                    sizesAfter.remove(key);
                                                    return String.format(
                                                            "%50s: from %d to %d (diff: %d)",
                                                            key, before, after, after - before);
                                                })
                                        .sorted()
                                        .collect(Collectors.joining("\n")),
                                sizesAfter.isEmpty()
                                        ? ""
                                        : String.format(
                                                """
                                                Added Graphs:
                                                %s
                                                """,
                                                sizesAfter.entrySet().stream()
                                                        .map(
                                                                e ->
                                                                        String.format(
                                                                                "%50s: size %d ",
                                                                                e.getKey(),
                                                                                e.getValue()))
                                                        .sorted()
                                                        .collect(Collectors.joining("\n"))));
                log.info(message);
            } else {
                long before = sizesBefore.get(Graphs.DEFAULT.getGraphName());
                long after = sizesAfter.get(Graphs.DEFAULT.getGraphName());
                log.info(
                        String.format(
                                "Filter %s changed statement count from %d to %d (diff: %d) in %d millis",
                                filter.getClass().getSimpleName(),
                                before,
                                after,
                                after - before,
                                end - start));
            }
        }
    }

    public void setLog(Log log) {
        this.log = log;
    }
}
