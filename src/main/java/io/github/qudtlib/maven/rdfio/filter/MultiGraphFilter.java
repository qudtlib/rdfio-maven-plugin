package io.github.qudtlib.maven.rdfio.filter;

import java.util.List;

public interface MultiGraphFilter extends Filter {
    List<String> getGraphs();
}
