package io.github.qudtlib.maven.rdfio.pipeline;

import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Inferred {
    private String graph;

    private String file;

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    // Inferred.java
    public static Inferred parse(Xpp3Dom config) {
        if (config == null || config.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    """
                            Inferred configuration is missing.
                            %s"""
                            .formatted(usage()));
        }

        Inferred inferred = new Inferred();

        ParsingHelper.optionalStringChild(config, "graph", inferred::setGraph, Inferred::usage);
        ParsingHelper.optionalStringChild(config, "file", inferred::setFile, Inferred::usage);
        if (inferred.getFile() == null && inferred.getGraph() == null) {
            throw new ConfigurationParseException(
                    """
                            Inferred must have at least one child element.
                            %s"""
                            .formatted(usage()));
        }
        return inferred;
    }

    public static String usage() {
        return """
                            Usage: Provide an <inferred> element with a <graph> and/or <file> element.
                            Example: <inferred><graph>inferred:graph</graph></inferred>""";
    }
}
