package io.github.qudtlib.maven.rdfio;

import static io.github.qudtlib.maven.rdfio.filter.RdfFormatHelper.supportsQuads;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.filter.Graphs;
import io.github.qudtlib.maven.rdfio.filter.GraphsHelper;
import io.github.qudtlib.maven.rdfio.filter.Input;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class AbstractRdfioMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project.basedir}", readonly = true)
    protected File basedir;

    protected void writeOutputToFile(
            String outputFile, Dataset dataset, List<String> graphs, String messageFormat)
            throws FileNotFoundException {
        if (outputFile != null) {
            File folder = new File(outputFile).getParentFile();
            if (!folder.exists()) {
                folder.mkdirs();
            }
            Lang lang = RDFLanguages.resourceNameToLang(outputFile, Lang.TTL);

            if (supportsQuads(lang)) {
                writeQuadsToFile(outputFile, dataset, graphs, lang);
            } else {
                if (GraphsHelper.hasNamedGraphs(dataset)) {
                    writeTriplesToFile(outputFile, dataset, graphs, lang);
                } else {
                    writeDefaultGraphToFile(outputFile, dataset, lang);
                }
            }
            getLog().info(String.format(messageFormat, outputFile));
        }
    }

    private void writeTriplesToFile(
            String outputFile, Dataset dataset, List<String> graphs, Lang lang)
            throws FileNotFoundException {
        String whichGraphs =
                "graph selection "
                        + graphs.stream().collect(Collectors.joining("', '", "['", "']"))
                        + " of";
        // if we write to a triples file, no selected graphs means default graph
        if (graphs.isEmpty()) {
            graphs.add(Graphs.DEFAULT.getGraphName());
        }
        GraphsHelper.retainSelected(dataset, graphs);
        getLog().info(
                        String.format(
                                "Writing union of %s RDF dataset to %s", whichGraphs, outputFile));
        Model toWrite = GraphsHelper.unionAll(dataset);
        deleteCarriageReturns(toWrite);
        RDFDataMgr.write(new FileOutputStream(new File(basedir, outputFile)), toWrite, lang);
    }

    private void writeDefaultGraphToFile(String outputFile, Dataset dataset, Lang lang)
            throws FileNotFoundException {
        Model defaultGraph = dataset.getDefaultModel();
        getLog().info(String.format("Writing all triples to " + outputFile));
        deleteCarriageReturns(defaultGraph);
        RDFDataMgr.write(new FileOutputStream(new File(basedir, outputFile)), defaultGraph, lang);
    }

    private void writeQuadsToFile(
            String outputFile, Dataset dataset, List<String> graphs, Lang lang)
            throws FileNotFoundException {
        String whichGraphs =
                "graph selection "
                        + graphs.stream().collect(Collectors.joining("', '", "['", "']"))
                        + " of";
        // if we write to a quads file, no selected graphs means all graphs
        if (graphs.isEmpty()) {
            whichGraphs = "complete";
            graphs.add(Graphs.EACH.getGraphName());
        }
        GraphsHelper.retainSelected(dataset, graphs);
        getLog().info(String.format("Writing %s RDF dataset to %s", whichGraphs, outputFile));
        deleteCarriageReturns(dataset);
        RDFDataMgr.write(new FileOutputStream(new File(basedir, outputFile)), dataset, lang);
    }

    private static void deleteCarriageReturns(Dataset dataset) {
        GraphsHelper.getAllModels(dataset).forEach(AbstractRdfioMojo::deleteCarriageReturns);
    }

    private static void deleteCarriageReturns(Model model) {
        Pattern containsR = Pattern.compile("\r", Pattern.MULTILINE);
        List<Statement> newStatements = new ArrayList<>();
        StmtIterator it = model.listStatements();
        while (it.hasNext()) {
            Statement s = it.nextStatement();
            RDFNode object = s.getObject();
            boolean statementReplaced = false;
            if (object.isLiteral()) {
                String stringValue;
                try {
                    stringValue = object.asLiteral().getString();
                    RDFDatatype rdfDatatype = object.asLiteral().getDatatype();
                    Matcher m = containsR.matcher(stringValue);
                    if (m.find()) {
                        stringValue = m.replaceAll("");
                        RDFNode newObject =
                                rdfDatatype == null
                                        ? ResourceFactory.createStringLiteral(stringValue)
                                        : ResourceFactory.createTypedLiteral(
                                                stringValue, rdfDatatype);
                        Statement newStatement =
                                new StatementImpl(s.getSubject(), s.getPredicate(), newObject);
                        newStatements.add(newStatement);
                        statementReplaced = true;
                    }
                } catch (Node.NotLiteral e) {
                    // that's ok - value is not a string
                }
            }
            if (!statementReplaced) {
                newStatements.add(s);
            }
        }
        model.removeAll();
        model.add(newStatements);
    }

    protected Dataset loadRdf(List<Input> inputs) throws MojoExecutionException {
        Dataset dataset = DatasetFactory.create();
        for (Input input : inputs) {
            String graphName = GraphsHelper.normalizeGraphName(input.getGraph());
            if (!Graphs.DEFAULT.getGraphName().equals(graphName)) {
                if (!dataset.containsNamedModel(graphName)) {
                    dataset.addNamedModel(graphName, ModelFactory.createDefaultModel());
                }
            }
            getLog().debug(String.format("Loading input data into graph %s", graphName));
            String[] files = FileHelper.getFilesForFileSelection(input, basedir);
            getLog().debug(String.format("Found %d files for this input", files.length));
            loadRdf(dataset, graphName, files);
        }
        return dataset;
    }

    protected void loadRdf(Dataset dataset, String graph, String[] files)
            throws MojoExecutionException {

        debug("Loading data into graph " + GraphsHelper.normalizeGraphName(graph));
        Model model = GraphsHelper.getModel(dataset, graph);
        long sizeBefore = model.size();
        for (String file : files) {
            loadRdfFromFile(file, model);
        }
        long sizeAfter = model.size();
        debug(
                "Loaded %d triples (before: %d, after: %d)",
                sizeAfter - sizeBefore, sizeBefore, sizeAfter);
    }

    protected void loadRdf(Dataset dataset, String graph, String file)
            throws MojoExecutionException {
        debug("Loading data into graph " + GraphsHelper.normalizeGraphName(graph));
        Model model = GraphsHelper.getModel(dataset, graph);
        long sizeBefore = model.size();
        loadRdfFromFile(file, model);
        long sizeAfter = model.size();
        debug(
                "Loaded %d triples (before: %d, after: %d)",
                sizeAfter - sizeBefore, sizeBefore, sizeAfter);
    }

    private void loadRdfFromFile(String file, Model model) throws MojoExecutionException {
        debug("Loading %s", file);
        File inFile = new File(basedir, file);
        Lang lang = RDFLanguages.resourceNameToLang(inFile.getName(), Lang.TTL);
        try {
            debug("RDF language: %s", lang);
            String content = Files.readString(inFile.toPath());
            debug("Content length: %d", content.length());
            ByteArrayInputStream inputStream =
                    new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            RDFParser.source(inputStream).lang(lang).parse(model);
        } catch (IOException e) {
            throw new MojoExecutionException(
                    "Error parsing RDF file " + inFile.getAbsolutePath(), e);
        }
    }

    protected void debug(String pattern, Object... args) {
        if (getLog().isDebugEnabled()) {
            getLog().debug(String.format(pattern, args));
        }
    }
}
