package io.github.qudtlib.maven.rdfio.pipeline;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public class WriteStep implements Step {
    @Parameter private String graph;

    @Parameter private String toFile;

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getToFile() {
        return toFile;
    }

    public void setToFile(String toFile) {
        this.toFile = toFile;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (graph == null) {
            throw new MojoExecutionException("Graph is required in write step");
        }
        Model model = dataset.getNamedModel(graph);
        String outputFile = toFile;
        if (outputFile == null) {
            Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
            StmtIterator it =
                    metaModel.listStatements(
                            null, RDFIO.loadsInto, ResourceFactory.createResource(graph));
            List<String> files = new ArrayList<>();
            while (it.hasNext()) {
                files.add(it.next().getSubject().getURI().replace("file://", ""));
            }
            if (files.size() == 1) {
                outputFile = files.get(0);
            } else if (files.isEmpty()) {
                throw new MojoExecutionException("No file mapping found for graph " + graph);
            } else {
                throw new MojoExecutionException("Multiple file mappings found for graph " + graph);
            }
        }
        try (FileOutputStream out = new FileOutputStream(outputFile)) {
            RDFDataMgr.write(out, model, Lang.TTL);
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to write file", e);
        }
        state.getPrecedingSteps().add(this);
    }

    @Override
    public String calculateHash(String previousHash) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("write".getBytes(StandardCharsets.UTF_8));
            if (graph != null) {
                digest.update(graph.getBytes(StandardCharsets.UTF_8));
            }
            if (toFile != null) {
                digest.update(toFile.getBytes(StandardCharsets.UTF_8));
            }
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
