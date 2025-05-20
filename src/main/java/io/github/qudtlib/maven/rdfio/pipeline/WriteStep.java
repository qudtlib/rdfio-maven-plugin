package io.github.qudtlib.maven.rdfio.pipeline;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class WriteStep implements Step {
    private final List<String> graphs = new ArrayList<>();

    private String toFile;

    public List<String> getGraphs() {
        return graphs;
    }

    public void addGraph(String graph) {
        this.graphs.add(graph);
    }

    public String getToFile() {
        return toFile;
    }

    public void setToFile(String toFile) {
        this.toFile = toFile;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (graphs.isEmpty()) {
            throw new MojoExecutionException("Graph is required in write step");
        }
        // possible cases
        // 1. no toGraph - each graph must have an associated file
        // 2. toGraph is a triples format (eg. xyz.ttl) - the union of all graphs is written to the
        // file
        // 3. tgGraph is a quads format (eg.  xyz.trig) - each graph is written to the file with the
        // graph uri as the fourth element
        String outputFileStr = toFile;
        if (outputFileStr == null) {
            writeOneFilePerGraph(dataset, state);
        } else {
            Lang outputLang = RDFLanguages.resourceNameToLang(outputFileStr, Lang.TTL);
            if (RDFLanguages.isQuads(outputLang)) {
                createParentFolder(state, outputFileStr);
                Dataset dsToWrite = DatasetFactory.create();
                for (String graph : graphs) {
                    dsToWrite.addNamedModel(graph, dataset.getNamedModel(graph));
                }
                try (FileOutputStream out = new FileOutputStream(outputFileStr)) {
                    RDFDataMgr.write(out, dsToWrite, outputLang);
                } catch (Exception e) {
                    throw new MojoExecutionException("Failed to write file", e);
                }
            } else {
                createParentFolder(state, outputFileStr);
                Model modelToWrite = ModelFactory.createDefaultModel();
                for (String graph : graphs) {
                    modelToWrite.add(dataset.getNamedModel(graph));
                }
                try (FileOutputStream out = new FileOutputStream(outputFileStr)) {
                    RDFDataMgr.write(out, modelToWrite, outputLang);
                } catch (Exception e) {
                    throw new MojoExecutionException("Failed to write file", e);
                }
            }
        }

        state.getPrecedingSteps().add(this);
    }

    private void writeOneFilePerGraph(Dataset dataset, PipelineState state)
            throws MojoExecutionException {
        String outputFileStr;
        for (String graph : this.graphs) {
            Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
            StmtIterator it =
                    metaModel.listStatements(
                            null, RDFIO.loadsInto, ResourceFactory.createResource(graph));
            List<String> files = new ArrayList<>();
            while (it.hasNext()) {
                files.add(it.next().getSubject().getURI().replace("file://", ""));
            }
            if (files.size() == 1) {
                outputFileStr = files.get(0);
            } else if (files.isEmpty()) {
                throw new MojoExecutionException("No file mapping found for graph " + graph);
            } else {
                throw new MojoExecutionException("Multiple file mappings found for graph " + graph);
            }
            createParentFolder(state, outputFileStr);
            try (FileOutputStream out = new FileOutputStream(outputFileStr)) {
                RDFDataMgr.write(out, dataset.getNamedModel(graph), Lang.TTL);
            } catch (Exception e) {
                throw new MojoExecutionException("Failed to write file", e);
            }
        }
    }

    private static void createParentFolder(PipelineState state, String outputFileStr)
            throws MojoExecutionException {
        File outputFile = new File(outputFileStr);
        state.requireUnderConfiguredDirs(outputFile);
        File outputFileParent = outputFile.getParentFile();
        if (!outputFileParent.exists()) {
            outputFileParent.mkdirs();
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("write".getBytes(StandardCharsets.UTF_8));
            graphs.forEach(graph -> digest.update(graph.getBytes(StandardCharsets.UTF_8)));

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

    // WriteStep.java
    public static WriteStep parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }

        WriteStep step = new WriteStep();
        ParsingHelper.requiredStringChildren(config, "graph", step::addGraph, WriteStep::usage);
        ParsingHelper.optionalStringChild(config, "toFile", step::setToFile, WriteStep::usage);
        return step;
    }

    public static String usage() {
        return """
                           Usage: Provide a <write> element with one or more <graph> and an optional <toFile> elements. The <toFile> element
                           can be omitted if the system knows which file each <graph> was read from, in which case it
                           will overwrite that file with the contents of the graph.
                           The toFile element can have a triples or quads file extension (eg '.ttl' or '.trig'). In the first case, the union of the specified
                           graphs is written to the file, in the latter, individual graphs are written to the file.
                           Example:
                           <write>
                               <graph>test:graph</graph>
                               <graph>test:graph2</graph>
                               <toFile>output.ttl</toFile>
                           </write>""";
    }
}
