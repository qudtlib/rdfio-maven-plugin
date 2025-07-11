package io.github.qudtlib.maven.rdfio;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.filter.Graphs;
import io.github.qudtlib.maven.rdfio.filter.GraphsHelper;
import io.github.qudtlib.maven.rdfio.filter.Input;
import io.github.qudtlib.maven.rdfio.product.EachFile;
import io.github.qudtlib.maven.rdfio.product.Product;
import io.github.qudtlib.maven.rdfio.product.Products;
import io.github.qudtlib.maven.rdfio.product.SingleFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "make", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class MakeMojo extends AbstractRdfioMojo {

    @Parameter(readonly = true, defaultValue = "target/rdfio-products")
    private String defaultOutputDir;

    public String getDefaultOutputDir() {
        return defaultOutputDir;
    }

    @Parameter(required = true)
    private Products products;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Making RDF files");
        SparqlHelper.registerNumericFunctions();
        Dataset dataset = DatasetFactory.create();
        FileSelection shaclFunctionFileSelection = products.getImportShaclFunctions();
        Model shaclFunctionsModel = null;
        if (shaclFunctionFileSelection != null) {
            importShaclFunctions(shaclFunctionFileSelection, dataset);
            shaclFunctionsModel =
                    dataset.getNamedModel(Graphs.SHACL_FUNCTIONS_GRAPH.getGraphName());
        }
        for (Product product : products.getProducts()) {
            try {
                if (product instanceof SingleFile) {
                    makeSingleFile((SingleFile) product, shaclFunctionsModel);
                } else if (product instanceof EachFile) {
                    makeEachFile((EachFile) product, shaclFunctionsModel);
                }
            } catch (FileNotFoundException e) {
                throw new MojoFailureException(
                        "Error making RDF file for product" + product.describe(), e);
            }
        }
    }

    private void importShaclFunctions(
            FileSelection shaclFunctionFileSelection, Dataset targetDataset)
            throws MojoExecutionException {
        String[] files = FileHelper.getFilesForFileSelection(shaclFunctionFileSelection, basedir);
        getLog().debug("Importing SHACL functions from " + Arrays.toString(files));
        loadRdf(targetDataset, Graphs.SHACL_FUNCTIONS_GRAPH.getGraphName(), files);
        SparqlHelper.registerShaclFunctions(
                targetDataset, Graphs.SHACL_FUNCTIONS_GRAPH.getGraphName(), getLog());
    }

    private void makeSingleFile(SingleFile singleFileProduct, Model shaclFunctionsModel)
            throws MojoFailureException, FileNotFoundException, MojoExecutionException {
        singleFileProduct.setLog(getLog());
        getLog().info("Make RDF files configuration:");
        writeInputConfiguration(singleFileProduct.getInputs());
        writeSingleFileOutputConfiguration(singleFileProduct);
        if (singleFileProduct.getOutputFile() == null) {
            throw new MojoFailureException(
                    "You must specify the name of the output file we are making!");
        }
        if (singleFileProduct.isSkip()) {
            getLog().info("Skip making RDF file " + singleFileProduct.getOutputFile());
            return;
        }
        debug("Loading data");
        Dataset dataset = loadRdf(singleFileProduct.getInputs());
        if (shaclFunctionsModel != null) {
            dataset.addNamedModel(Graphs.SHACL_FUNCTIONS_GRAPH.getGraphName(), shaclFunctionsModel);
        }
        singleFileProduct.process(dataset);
        writeOutputToFile(
                singleFileProduct.getOutputFile(),
                dataset,
                singleFileProduct.getGraphs(),
                "writing RDF data to %s");
    }

    private void writeSingleFileOutputConfiguration(SingleFile singleFileProduct) {
        String graphSelectionInfo =
                "(Graphs: "
                        + singleFileProduct.getGraphs().stream()
                                .collect(Collectors.joining(", ", "[", "]"))
                        + ")";
        if (GraphsHelper.isOnlyDefaultGraph(singleFileProduct.getGraphs())) {
            graphSelectionInfo = "";
        }
        getLog().info(
                        String.format(
                                "output%s: %s ",
                                graphSelectionInfo, singleFileProduct.getOutputFile()));
    }

    private void writeInputConfiguration(List<Input> inputs) {
        Set<String> lines = new HashSet<>();
        Set<String> noGraphLines = new HashSet<>();
        Set<String> allGraphs = new HashSet<>();
        for (Input input : inputs) {
            String graphName = GraphsHelper.normalizeGraphName(input.getGraph());
            allGraphs.add(graphName);
            String prefix =
                    String.format(
                            "    Graph %s",
                            Graphs.DEFAULT.getGraphName().equals(graphName)
                                    ? "(default graph)"
                                    : "'" + graphName + "'");
            String[] inputFiles = FileHelper.getFilesForFileSelection(input, basedir);
            lines.addAll(Arrays.stream(inputFiles).sorted().map(f -> prefix + ": " + f).toList());
            noGraphLines.addAll(Arrays.stream(inputFiles).sorted().map(f -> "    " + f).toList());
        }
        getLog().info("input:");
        if (GraphsHelper.isOnlyDefaultGraph(allGraphs)) {
            noGraphLines.stream().sorted().forEach(getLog()::info);
        } else {
            lines.stream().sorted().forEach(getLog()::info);
        }
    }

    private void makeEachFile(EachFile eachFileProduct, Model shaclFunctionsModel)
            throws MojoFailureException, FileNotFoundException, MojoExecutionException {
        eachFileProduct.setLog(getLog());
        getLog().info("Make RDF files configuration:");
        writeInputConfiguration(eachFileProduct.getInputs());
        String outputDir =
                Optional.ofNullable(eachFileProduct.getOutputDir()).orElse(getDefaultOutputDir());
        if (eachFileProduct.isReplaceInputFiles()) {
            getLog().info("output: input files are overwritten");
            if (eachFileProduct.getOutputDir() != null
                    && !eachFileProduct.getOutputDir().isBlank()) {
                getLog().warn(
                                "Parameter 'outputDir', which is provided, is ignored because 'replaceInputFiles' is 'true'");
            }
        } else {
            getLog().info("output dir: " + outputDir);
        }
        if (eachFileProduct.isSkip()) {
            getLog().info("Skip making RDF file(s)");
            return;
        }
        for (Input input : eachFileProduct.getInputs()) {
            String[] inputFiles = FileHelper.getFilesForFileSelection(input, basedir);
            for (String inputFile : inputFiles) {
                debug("Loading data");
                Dataset dataset = DatasetFactory.create();
                loadRdf(dataset, GraphsHelper.normalizeGraphName(input.getGraph()), inputFile);
                if (shaclFunctionsModel != null) {
                    dataset.addNamedModel(
                            Graphs.SHACL_FUNCTIONS_GRAPH.getGraphName(), shaclFunctionsModel);
                }
                eachFileProduct.process(dataset);
                if (eachFileProduct.isReplaceInputFiles()) {
                    writeOutputToFile(
                            inputFile,
                            dataset,
                            eachFileProduct.getGraphs(),
                            "writing RDF data back to input file %s");
                } else {
                    String outputFile =
                            new File(outputDir, new File(inputFile).getName()).toString();
                    writeOutputToFile(
                            outputFile,
                            dataset,
                            eachFileProduct.getGraphs(),
                            "writing RDF data to %s");
                }
            }
        }
    }
}
