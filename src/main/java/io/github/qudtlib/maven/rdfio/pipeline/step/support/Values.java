package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
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
    public static Values parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            Values configuration is missing.
                            Usage: Provide a <values> element with a <graphs> selection.
                            Example: <values><graphs><include>vocab:*</include></graphs></values>""");
        }

        Values values = new Values();
        Xpp3Dom graphsDom = config.getChild("graphs");
        if (graphsDom == null) {
            throw new ConfigurationParseException(
                    """
                            Values requires a <graphs> element.
                            Usage: Specify a graph selection via <graphs>.
                            Example: <graphs><include>vocab:*</include></graphs>""");
        }
        values.setGraphs(GraphSelection.parse(graphsDom));
        return values;
    }
}
