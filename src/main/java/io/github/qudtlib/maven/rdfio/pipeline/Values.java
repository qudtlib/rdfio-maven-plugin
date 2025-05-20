package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Values {
    @Parameter private GraphSelection graphs;

    public GraphSelection getGraphs() {
        return graphs;
    }

    public void setGraphs(GraphSelection graphs) {
        this.graphs = graphs;
    }

    // Values.java
    public static Values parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            Values configuration is missing.
                            Usage: Provide a <values> element with a <graphs> selection.
                            Example: <values><graphs><include>vocab:*</include></graphs></values>""");
        }

        Values values = new Values();
        Xpp3Dom graphsDom = config.getChild("graphs");
        if (graphsDom == null) {
            throw new MojoExecutionException(
                    """
                            Values requires a <graphs> element.
                            Usage: Specify a graph selection via <graphs>.
                            Example: <graphs><include>vocab:*</include></graphs>""");
        }
        values.setGraphs(GraphSelection.parse(graphsDom));
        return values;
    }
}
