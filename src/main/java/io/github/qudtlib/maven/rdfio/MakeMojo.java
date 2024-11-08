package io.github.qudtlib.maven.rdfio;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "make", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class MakeMojo extends AbstractRdfioMojo {

    @Parameter(required = true)
    private List<SingleFile> products;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Making RDF files");
        for (SingleFile singleFile : products) {
            try {
                makeSingleFile(singleFile);
            } catch (FileNotFoundException e) {
                throw new MojoFailureException(
                        "Error making RDF file " + singleFile.getOutputFile(), e);
            }
        }
    }

    private void makeSingleFile(SingleFile singleFileProduct)
            throws MojoFailureException, FileNotFoundException, MojoExecutionException {

        getLog().info("Make RDF files configuration:");
        String[] inputFiles = getFilesForPatterns(singleFileProduct.getInput());
        getLog().info(
                        "\tinput: "
                                + Arrays.stream(inputFiles)
                                        .collect(Collectors.joining("\n\t", "\n\t", "\n")));
        getLog().info("\toutput: " + singleFileProduct.getOutputFile());
        if (singleFileProduct.getOutputFile() == null) {
            throw new MojoFailureException(
                    "You must specify the name of the output file we are making!");
        }
        if (singleFileProduct.isSkip()) {
            getLog().info("Skip making RDF file " + singleFileProduct.getOutputFile());
            return;
        }
        debug("Loading data");
        Model model = loadRdf(inputFiles);
        singleFileProduct.filter(model);
        writeModelToFile(singleFileProduct.getOutputFile(), model, "writing RDF data to %s");
    }
}
