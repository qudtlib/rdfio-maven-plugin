package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Values {
    @Parameter private GraphSelection graphs;

    private String in;

    private List<String> inList = null;

    public GraphSelection getGraphs() {
        return graphs;
    }

    public void setGraphs(GraphSelection graphs) {
        this.graphs = graphs;
    }

    private String getIn() {
        return in;
    }

    private void setIn(String in) {
        this.in = in;
    }

    public List<String> getInList() {
        return inList;
    }

    public void setInList(List<String> inList) {
        this.inList = inList;
    }

    public List<RDFNode> getValues(Dataset dataset, PipelineState state) {
        if (this.inList != null) {
            return this.inList.stream()
                    .map(s -> ResourceFactory.createStringLiteral(s))
                    .collect(Collectors.toUnmodifiableList());
        }
        if (this.getGraphs() != null) {
            return PipelineHelper.getGraphs(dataset, this.getGraphs(), state).stream()
                    .map(s -> ResourceFactory.createResource(s))
                    .collect(Collectors.toUnmodifiableList());
        }
        throw new IllegalStateException("neither inList or graphs is set!");
    }

    // Values.java
    public static Values parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(config, usage());
        }

        Values values = new Values();
        ParsingHelper.optionalStringChild(config, "in", values::setIn, Values::usage);
        if (values.getIn() != null) {
            String[] valuesArr = FileHelper.splitPatterns(values.getIn());
            values.inList = Arrays.asList(valuesArr);
        }
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, values::setGraphs, Values::usage);
        Xpp3Dom graphsDom = config.getChild("graphs");
        if (values.getGraphs() == null && values.getInList() == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            <values> requires a <graphs> or an <in> element.
                """);
        }
        if (values.getGraphs() != null && values.getInList() != null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            <values> subelements <graphs> and <in> cannot both be set.
                """);
        }
        return values;
    }

    public static String usage() {
        return """
                Usage: Specify a list of graphs via patterns or an explicit list
                    - <graphs>: a graph selection via <include> and <exclude>.
                    - <in>: a list of strings, one per line or comma-separated
                Example: <graphs><include>vocab:*</include></graphs>
                """;
    }

    public void updateHash(MessageDigest digest, PipelineState state) {
        digest.update("values".getBytes(StandardCharsets.UTF_8));
        if (this.graphs != null) {
            this.graphs.updateHash(digest, state);
        }
        if (this.in != null) {
            digest.update(this.in.getBytes(StandardCharsets.UTF_8));
        }
    }
}
