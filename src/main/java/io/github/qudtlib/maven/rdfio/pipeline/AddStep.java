package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class AddStep implements Step {
    private static final Set<String> KNOWN_VARIABLES = Set.of("name", "path", "index");
    private final List<String> files = new ArrayList<>();

    private FileSelection fileSelection;

    private final List<String> graphs = new ArrayList<>();

    private String toGraph;

    private String toGraphsPattern;

    private GraphSelection graphSelection;

    public List<String> getFiles() {
        return files;
    }

    public void addFile(String file) {
        this.files.add(file);
    }

    public FileSelection getFileSelection() {
        return fileSelection;
    }

    public void setFileSelection(FileSelection fileSelection) {
        this.fileSelection = fileSelection;
    }

    public List<String> getGraphs() {
        return graphs;
    }

    public void addGraph(String graph) {
        this.graphs.add(graph);
    }

    public void setGraphSelection(GraphSelection graphSelection) {
        this.graphSelection = graphSelection;
    }

    public GraphSelection getGraphSelection() {
        return graphSelection;
    }

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
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            Add step configuration is missing.
                            Usage: Provide an <add> element with either <file> or <files> and a target graph via <toGraph> or <toGraphsPattern>.
                            Example:
                            <add>
                                <file>data.ttl</file>
                                <toGraph>test:graph</toGraph>
                            </add>""");
        }

        AddStep step = new AddStep();
        ParsingHelper.optionalStringChildren(config, "file", step::addFile, WriteStep::usage);
        ParsingHelper.optionalStringChildren(config, "graph", step::addGraph, WriteStep::usage);
        ParsingHelper.optionalDomChild(
                config, "files", FileSelection::parse, step::setFileSelection, WriteStep::usage);
        ParsingHelper.optionalDomChild(
                config, "graphs", GraphSelection::parse, step::setGraphSelection, WriteStep::usage);
        if (step.getGraphs().isEmpty()
                && step.getGraphSelection() == null
                && step.getFiles().isEmpty()
                && step.getFileSelection() == null) {
            throw new ConfigurationParseException(
                    "Add step requires one of <file>, <files>, <graph>, or <graphs>.\n"
                            + step.usage());
        }
        ParsingHelper.optionalStringChild(config, "toGraph", step::setToGraph, WriteStep::usage);
        ParsingHelper.optionalStringChild(
                config, "toGraphsPattern", step::setToGraphsPattern, WriteStep::usage);
        if (step.getToGraph() == null && step.getToGraphsPattern() == null) {
            throw new ConfigurationParseException(
                    """
                            Add step requires one of <toGraph> or <toGraphsPattern>.
                            Usage: Specify a target graph or a graph pattern.
                            Examples:
                            - Target graph: <toGraph>test:graph</toGraph>
                            - Graph pattern: <toGraphsPattern>test:graph-{0}</toGraphsPattern>""");
        }
        if (step.getToGraph() != null) {
            step.setToGraphsPattern(null);
        }
        Pattern p = Pattern.compile("\\$\\{([^/: ]+)}");
        if (step.getToGraphsPattern() != null) {
            Matcher m = p.matcher(step.getToGraphsPattern());
            if (m.find() && !KNOWN_VARIABLES.contains(m.group(1).toLowerCase(Locale.ROOT))) {

                String unknownVar = m.group(0);
                throw new ConfigurationParseException(
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
        return step;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        File baseDir = state.getBaseDir();
        List<File> inputFiles = RdfFileProcessor.resolveFiles(files, fileSelection, baseDir);
        int index = 0;
        if (!inputFiles.isEmpty()) {
            if (toGraph == null && toGraphsPattern == null) {
                throw new MojoExecutionException("Missing toGraph or toGraphsPattern in add step");
            }
            for (File inputFile : inputFiles) {
                String inputFilePath = FileHelper.relativizeAsUnixStyle(baseDir, inputFile);
                String tgp =
                        replaceVariables(
                                getToGraphsPattern(), inputFilePath, inputFile.getName(), index);
                String targetGraph = toGraph != null ? toGraph : tgp;

                Model model = dataset.getNamedModel(targetGraph);
                List<File> files = List.of(inputFile);
                FileHelper.ensureFilesExist(files, "input");
                RdfFileProcessor.loadRdfFiles(List.of(inputFile), model);
                Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
                metaModel.add(
                        FileHelper.getFileUrl(inputFile),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource(targetGraph));
                index++;
            }
        }
        List<String> inputGraphs = this.graphs;
        PipelineHelper.ensureGraphsExist(dataset, inputGraphs, "input graph");
        inputGraphs.addAll(PipelineHelper.getGraphs(dataset, graphSelection));
        if (!inputGraphs.isEmpty()) {
            if (toGraph != null) {
                Model targetModel = dataset.getNamedModel(toGraph);
                for (String sourceGraph : inputGraphs) {
                    requireSourceGraphExists(dataset, sourceGraph);
                    Model sourceModel = dataset.getNamedModel(sourceGraph);
                    if (sourceModel != null) {
                        targetModel.add(sourceModel);
                    }
                }
            } else {
                for (String sourceGraph : inputGraphs) {
                    String targetGraph =
                            replaceVariables(
                                    getToGraphsPattern(), sourceGraph, getName(sourceGraph), index);
                    requireSourceGraphExists(dataset, sourceGraph);
                    Model sourceModel = dataset.getNamedModel(sourceGraph);
                    Model targetModel = dataset.getNamedModel(targetGraph);
                    if (sourceModel != null) {
                        targetModel.add(sourceModel);
                    }
                    index++;
                }
            }
        }
        state.getPrecedingSteps().add(this);
    }

    private static void requireSourceGraphExists(Dataset dataset, String sourceGraph)
            throws MojoExecutionException {
        if (!dataset.containsNamedModel(sourceGraph)) {
            throw new MojoExecutionException(
                    "source graph of <add> element '%s' not found in dataset"
                            .formatted(sourceGraph));
        }
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

    public static String usage() {
        return """
                Usage: Specify
                    - inputs: using <file>, <files>, <graph> or <graphs>
                    - target graph using <toGraph> or one graph per input using <toGraphsPattern>
                       in <toGraphPattern>,
                        '${name}' is replaced with the last part of the input file/graph
                        '${path}' is replaced with the whole input file/graph
                        '${index}' is replaced with the 1-based index of the file/graph being loaded
                Examples:
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
            RdfFileProcessor.updateHashWithFiles(
                    RdfFileProcessor.resolveFiles(files, fileSelection, state.getBaseDir()),
                    digest);
            if (fileSelection != null) {
                for (String include : fileSelection.getInclude()) {
                    digest.update(include.getBytes(StandardCharsets.UTF_8));
                }
                for (String exclude : fileSelection.getExclude()) {
                    digest.update(exclude.getBytes(StandardCharsets.UTF_8));
                }
            }
            for (String g : graphs) {
                digest.update(g.getBytes(StandardCharsets.UTF_8));
            }
            if (toGraph != null) {
                digest.update(toGraph.getBytes(StandardCharsets.UTF_8));
            }
            if (toGraphsPattern != null) {
                digest.update(toGraphsPattern.getBytes(StandardCharsets.UTF_8));
            }
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
