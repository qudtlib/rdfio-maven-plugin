package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public class AddStep implements Step {
    @Parameter private List<String> file = new ArrayList<>();

    @Parameter private FileSelection files;

    @Parameter private List<String> graph = new ArrayList<>();

    @Parameter private String toGraph;

    @Parameter private String toGraphsPattern;

    public List<String> getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file.add(file);
    }

    public FileSelection getFiles() {
        return files;
    }

    public void setFiles(FileSelection files) {
        this.files = files;
    }

    public List<String> getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph.add(graph);
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
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        File baseDir = state.getBaseDir();
        List<File> inputFiles = RdfFileProcessor.resolveFiles(file, files, baseDir);
        if (!inputFiles.isEmpty()) {
            if (toGraph == null && toGraphsPattern == null) {
                throw new MojoExecutionException("Missing toGraph or toGraphsPattern in add step");
            }
            for (File inputFile : inputFiles) {
                String inputFilePath = inputFile.getPath().replace(File.separatorChar, '/');
                String targetGraph =
                        toGraph != null
                                ? toGraph
                                : toGraphsPattern
                                        .replace("$filePath", inputFilePath)
                                        .replace("$fileName", inputFile.getName());
                Model model = dataset.getNamedModel(targetGraph);
                RdfFileProcessor.loadRdfFiles(List.of(inputFile), model);
                Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
                metaModel.add(
                        ResourceFactory.createResource("file://" + inputFilePath),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource(targetGraph));
            }
        }
        if (!graph.isEmpty() && toGraph != null) {
            Model targetModel = dataset.getNamedModel(toGraph);
            for (String sourceGraph : graph) {
                Model sourceModel = dataset.getNamedModel(sourceGraph);
                if (sourceModel != null) {
                    targetModel.add(sourceModel);
                }
            }
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("add".getBytes(StandardCharsets.UTF_8));
            RdfFileProcessor.updateHashWithFiles(
                    RdfFileProcessor.resolveFiles(file, files, state.getBaseDir()), digest);
            if (files != null) {
                for (String include : files.getInclude()) {
                    digest.update(include.getBytes(StandardCharsets.UTF_8));
                }
                for (String exclude : files.getExclude()) {
                    digest.update(exclude.getBytes(StandardCharsets.UTF_8));
                }
            }
            for (String g : graph) {
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
