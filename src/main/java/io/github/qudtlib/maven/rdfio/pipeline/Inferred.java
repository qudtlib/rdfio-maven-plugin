package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.maven.plugin.MojoExecutionException;
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
    public static Inferred parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Inferred configuration is missing.
                            Usage: Provide an <inferred> element with a <graph> and/or <file> element.
                            Example: <inferred><graph>inferred:graph</graph></inferred>""");
        }

        Inferred inferred = new Inferred();

        inferred.setGraph(ParsingHelper.getNonBlankChildString(config, "graph"));
        inferred.setFile(ParsingHelper.getNonBlankChildString(config, "file"));
        if (inferred.getFile() == null && inferred.getGraph() == null) {
            throw new MojoExecutionException(
                    """
                            Inferred must h
                            ave at least one child element.
                            Usage: Provide an <inferred> element with a <graph> and/or <file> element.
                            Example: <inferred><graph>inferred:graph</graph></inferred>""");
        }
        return inferred;
    }
}
