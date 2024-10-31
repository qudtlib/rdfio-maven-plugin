package io.github.qudtlib.maven.rdfio;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "infer", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class MakeMojo extends AbstractRdfioMojo {

    @Parameter(required = true)
    private List<SingleFile> products;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Making RDF files");
        for (SingleFile check : products) {
            try {
                performShaclInference(check);
            } catch (FileNotFoundException e) {
                throw new MojoFailureException("Error making RDF files", e);
            }
        }
    }

    private void performShaclInference(SingleFile singleFileProduct)
            throws MojoFailureException, FileNotFoundException {
        getLog().info("Make RDF files configuration:");
        String[] inputFiles = getFilesForPatterns(singleFileProduct.getInput());
        getLog().info(
                        "\tshapes: "
                                + Arrays.stream(inputFiles)
                                        .collect(Collectors.joining("\n\t", "\n\t", "\n")));
        getLog().info("\toutput: " + singleFileProduct.getOutputFile());
        if (singleFileProduct.getOutputFile() == null) {
            throw new MojoFailureException(
                    "You must specify an output file for the inferred triples!");
        }
        if (singleFileProduct.isSkip()) {
            getLog().info("Skipping making single RDF file");
            return;
        }
        debug("Loading data");
        Graph inputGraph = loadRdf(inputFiles);
        writeModelToFile(
                singleFileProduct.getOutputFile(),
                ModelFactory.createModelForGraph(inputGraph),
                "writing RDF data to %s");
    }
}
