package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class GraphSelection {
    private final List<String> include = new ArrayList<>();

    private final List<String> exclude = new ArrayList<>();

    public GraphSelection() {}

    public GraphSelection(List<String> include, List<String> exclude) {
        this.include.addAll(include);
        this.exclude.addAll(exclude);
    }

    public List<String> getInclude() {
        return include;
    }

    public void addInclude(String include) {
        this.include.add(include);
    }

    public List<String> getExclude() {
        return exclude;
    }

    public void addExclude(String exclude) {
        this.exclude.add(exclude);
    }

    // GraphSelection.java
    public static GraphSelection parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            GraphSelection configuration is missing.
                            Usage: Provide a <graphs> element with at least one <include> pattern.
                            Example: <graphs><include>vocab:*</include></graphs>""");
        }

        GraphSelection selection = new GraphSelection();
        Xpp3Dom[] includeDoms = config.getChildren("include");
        if (includeDoms.length == 0) {
            throw new ConfigurationParseException(
                    """
                            GraphSelection requires at least one <include> pattern.
                            Usage: Specify one or more graph URI patterns.
                            Example: <include>vocab:*</include>""");
        }
        for (Xpp3Dom includeDom : includeDoms) {
            if (includeDom.getValue() != null && !includeDom.getValue().trim().isEmpty()) {
                selection.addInclude(includeDom.getValue().trim());
            } else {
                throw new ConfigurationParseException(
                        """
                                Empty or missing <include> pattern in GraphSelection.
                                Usage: Provide a non-empty graph URI pattern.
                                Example: <include>vocab:*</include>""");
            }
        }

        Xpp3Dom[] excludeDoms = config.getChildren("exclude");
        for (Xpp3Dom excludeDom : excludeDoms) {
            if (excludeDom.getValue() != null && !excludeDom.getValue().trim().isEmpty()) {
                selection.addExclude(excludeDom.getValue().trim());
            }
        }

        return selection;
    }

    public boolean isEmpty() {
        return this.include.isEmpty() && this.exclude.isEmpty();
    }
}
