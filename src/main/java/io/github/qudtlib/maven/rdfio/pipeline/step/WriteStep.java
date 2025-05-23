package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
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
    public String getElementName() {
        return "write";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {

        // possible cases
        // 1. no toFile - each graph must have an associated file
        // 2. toFile is a triples format (eg. xyz.ttl) - the union of all graphs is written to the
        // file
        // 3. toFile is a quads format (eg.  xyz.trig) - each graph is written to the file with the
        // graph uri as the fourth element
        if (this.toFile == null) {
            writeOneFilePerGraph(dataset, state);
        } else {
            RelativePath outputPath =
                    state.files().make(state.variables().resolve(this.toFile, dataset));
            Lang outputLang = RDFLanguages.resourceNameToLang(outputPath.getName(), Lang.TTL);
            if (RDFLanguages.isQuads(outputLang)) {
                state.files().createParentFolder(outputPath);
                Dataset dsToWrite = DatasetFactory.create();
                if (graphs.isEmpty()) {
                    dsToWrite.getDefaultModel().add(dataset.getDefaultModel());
                } else {
                    for (String graph : state.variables().resolve(graphs, dataset)) {
                        dsToWrite.addNamedModel(graph, dataset.getNamedModel(graph));
                    }
                }
                state.files().writeRdf(outputPath, dsToWrite);
            } else {
                state.files().createParentFolder(outputPath);
                Model modelToWrite = ModelFactory.createDefaultModel();
                if (graphs.isEmpty()) {
                    modelToWrite.add(dataset.getDefaultModel());
                } else {
                    for (String graph : state.variables().resolve(graphs, dataset)) {
                        modelToWrite.add(dataset.getNamedModel(graph));
                    }
                }
                state.files().writeRdf(outputPath, modelToWrite);
            }
        }

        state.getPrecedingSteps().add(this);
    }

    private void writeOneFilePerGraph(Dataset dataset, PipelineState state)
            throws MojoExecutionException {
        if (this.graphs.isEmpty()) {
            throw new MojoExecutionException(
                    "Neither <graph> nor <toFile> is specified - that is not enough\n%s"
                            .formatted(usage()));
        }
        String outputFileStr;
        for (String graph : state.variables().resolve(this.graphs, dataset)) {
            Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
            StmtIterator it =
                    metaModel.listStatements(
                            null, RDFIO.loadsInto, ResourceFactory.createResource(graph));
            List<String> files = new ArrayList<>();
            while (it.hasNext()) {
                String graphPath = it.next().getSubject().toString();
                files.add(graphPath);
            }
            if (files.size() == 1) {
                outputFileStr = files.get(0);
            } else if (files.isEmpty()) {
                throw new MojoExecutionException("No file mapping found for graph " + graph);
            } else {
                throw new MojoExecutionException("Multiple file mappings found for graph " + graph);
            }
            RelativePath outputPath = state.files().make(outputFileStr);
            state.files().createParentFolder(outputPath);
            state.files().writeRdf(outputPath, dataset.getNamedModel(graph));
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
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    // WriteStep.java
    public static WriteStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }

        WriteStep step = new WriteStep();
        ParsingHelper.optionalStringChildren(config, "graph", step::addGraph, WriteStep::usage);
        ParsingHelper.optionalStringChild(config, "toFile", step::setToFile, WriteStep::usage);
        if (step.graphs.isEmpty()) {
            if (step.toFile == null) {
                throw new ConfigurationParseException(
                        """
                            No <graph> is specified, data is taken from the default graph.
                            In this case <toFile> must be specified
                            %s"""
                                .formatted(usage()));
            }
        }
        return step;
    }

    public static String usage() {
        return """
                           Usage:

                            Provide a <write> element with optional <graph>s and one optional <toFile> elements.

                            If <graph> is omitted, <toFile> must be present - in this case the content of the default
                            graph is written to the file.

                            The <toFile> element can be omitted if the system knows which file each <graph>
                            was read from, in which case it will overwrite that file with the contents of the graph.

                            The toFile element can have a triples or quads file extension (eg '.ttl' or '.trig').
                            In the first case, the union of the specified graphs is written to the file, in the latter,
                            individual graphs are written to the file.

                           Example:
                           <write>
                               <graph>test:graph</graph>
                               <graph>test:graph2</graph>
                               <toFile>output.ttl</toFile>
                           </write>""";
    }
}
