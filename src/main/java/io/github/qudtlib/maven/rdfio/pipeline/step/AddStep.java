package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.InputsComponent;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import io.github.qudtlib.maven.rdfio.pipeline.support.PipelineConfigurationExeception;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class AddStep implements Step {
    private static final Set<String> KNOWN_VARIABLES = Set.of("name", "path", "index");
    private InputsComponent<AddStep> inputsComponent = new InputsComponent<>(this);

    public InputsComponent<AddStep> getInputsComponent() {
        return inputsComponent;
    }

    public void setInputsComponent(InputsComponent<AddStep> inputsComponent) {
        this.inputsComponent = inputsComponent;
    }

    private String toGraph;

    private String toGraphsPattern;

    public String getToGraph() {
        return toGraph;
    }

    public void setToGraph(String toGraph) {
        this.toGraph = toGraph;
    }

    public String getToGraphsPattern() {
        return toGraphsPattern;
    }

    public void setToGraphsPattern(String toGraphsPattern) {
        this.toGraphsPattern = toGraphsPattern;
    }

    @Override
    public String getElementName() {
        return "add";
    }

    public static AddStep parse(Xpp3Dom config) {
        AddStep step = new AddStep();
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Add step configuration is missing.
                            %s"""
                            .formatted(step.usage()));
        }

        ParsingHelper.optionalDomComponent(
                config,
                InputsComponent.getParseFunction(step),
                step::setInputsComponent,
                step::usage);
        ParsingHelper.optionalStringChild(config, "toGraph", step::setToGraph, step::usage);
        ParsingHelper.optionalStringChild(
                config, "toGraphsPattern", step::setToGraphsPattern, step::usage);
        if (step.getToGraph() != null) {
            step.setToGraphsPattern(null);
        }
        Pattern p = Pattern.compile("\\$\\{([^/: ]+)}");
        if (step.getToGraphsPattern() != null) {
            Matcher m = p.matcher(step.getToGraphsPattern());
            if (m.find() && !KNOWN_VARIABLES.contains(m.group(1).toLowerCase(Locale.ROOT))) {

                String unknownVar = m.group(0);
                throw new ConfigurationParseException(
                        config,
                        String.format(
                                """
                                Encountered variable %s, which is not supported."
                                Allowed variables in the toGraphsPattern are:
                                   - ${path}:   the whole file path or graph URI as provided, including the last bit
                                   - ${name}:   only the last bit of the file path/graph URI
                                   - ${index}:  the 0-based index of the input).
                                """,
                                unknownVar));
            }
        }
        if (step.getInputsComponent().hasNoInputs()
                && step.getToGraphsPattern() == null
                && step.getToGraph() == null) {
            throw new PipelineConfigurationExeception(
                    "<add> has neither inputs nor outputs!\n%s".formatted(step.usage()));
        }
        return step;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        String toGraphResolved = state.variables().resolve(toGraph, dataset);
        String toGraphsPatternResolved = state.variables().resolve(toGraphsPattern, dataset);
        if (inputsComponent.hasNoInputs()) {
            if (toGraphsPatternResolved == null && toGraphResolved == null) {
                throw new PipelineConfigurationExeception(
                        "<add> has neither inputs nor outputs!\n%s".formatted(usage()));
            }
            if (toGraphResolved == null) {
                throw new PipelineConfigurationExeception(
                        "<add> has no inputs - it needs at least a <toGraph> element, so we can copy the triples from the default graph there.\n%s"
                                .formatted(usage()));
            }
            state.getLog().debug("Loading default graph into graph %s".formatted(toGraphResolved));
            // take default model and put it in the toGraph
            PipelineHelper.addDefaultModelToGraph(dataset, state, toGraphResolved);
        } else {
            List<RelativePath> inputFiles = inputsComponent.getAllInputPaths(dataset, state);
            int index = 0;
            if (!inputFiles.isEmpty()) {
                for (RelativePath inputPath : inputFiles) {
                    String inputFilePath = inputPath.getRelativePath();
                    boolean isBijectiveFileToGraphRel = false;
                    String targetGraph = null;
                    if (toGraphResolved != null) {
                        if (inputFiles.size() == 1) {
                            isBijectiveFileToGraphRel = true;
                        }
                        targetGraph = toGraphResolved;
                    } else if (getToGraphsPattern() != null) {
                        String tgp =
                                replaceVariables(
                                        getToGraphsPattern(),
                                        inputFilePath,
                                        inputPath.getName(),
                                        index);
                        if (!tgp.equals(getToGraphsPattern())) {
                            // the replace did change something - each file gets its own graph
                            isBijectiveFileToGraphRel = true;
                        }
                        targetGraph = tgp;
                    } else {
                        targetGraph = null; // will write to default graph
                    }
                    PipelineHelper.readFileToGraph(
                            dataset, state, inputPath, targetGraph, isBijectiveFileToGraphRel);
                    index++;
                }
            }
            List<String> inputGraphs = inputsComponent.getAllInputGraphs(dataset, state);
            if (!inputGraphs.isEmpty()) {
                if (toGraphResolved != null) {
                    PipelineHelper.addGraphsToGraph(dataset, toGraphResolved, inputGraphs, state);
                } else if (toGraphsPatternResolved != null) {
                    for (String sourceGraph : inputGraphs) {
                        String targetGraph =
                                replaceVariables(
                                        getToGraphsPattern(),
                                        sourceGraph,
                                        getName(sourceGraph),
                                        index);
                        PipelineHelper.addGraphToGraph(dataset, sourceGraph, targetGraph, state);
                        index++;
                    }
                } else {
                    PipelineHelper.addGraphsToDefaultGraph(dataset, inputGraphs, state);
                }
            }
        }
        state.getPrecedingSteps().add(this);
    }

    private static String getName(String sourceGraph) {
        return sourceGraph.replaceFirst("^.*([^/\\\\: ]+)$", "");
    }

    private static String replaceVariables(
            String toGraphsPattern, String path, String name, int index) {
        if (toGraphsPattern == null) {
            return null;
        }
        return toGraphsPattern
                .replaceAll("\\$\\{path}", path)
                .replaceAll("\\$\\{name}", name)
                .replaceAll("\\$\\{index}", Integer.toString(index));
    }

    public String usage() {
        return """
                Usage: Specify
                    - inputs: using <file>, <files>, <graph> or <graphs>
                    - target graph using <toGraph> or one graph per input using <toGraphsPattern>
                       in <toGraphPattern>:
                        '${name}' is replaced with the last part of the input file/graph
                        '${path}' is replaced with the whole input file/graph
                        '${index}' is replaced with the 1-based index of the file/graph being loaded
                    - NOTE: no target means adding everything to the default graph
                Examples:
                - <add>  <!-- writes to the default graph -->
                        <file>/src/main/resources/myinput.ttl</file>
                   </add>
                 - <add>
                        <file>/src/main/resources/myinput.ttl</file>
                        <toGraph>test:graph</toGraph>
                   </add>
                 - <add>
                        <files>
                            <include>/src/main/resources/**/*.ttl</include>
                        </files>
                        <toGraphPattern>test:graph:$name</toGraph>
                   </add>
               """;
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("add".getBytes(StandardCharsets.UTF_8));
            inputsComponent.updateHash(digest, state);
            if (toGraph != null) {
                digest.update(toGraph.getBytes(StandardCharsets.UTF_8));
            }
            if (toGraphsPattern != null) {
                digest.update(toGraphsPattern.getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
