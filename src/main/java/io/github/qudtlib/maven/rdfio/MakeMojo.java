package io.github.qudtlib.maven.rdfio;

import io.github.qudtlib.maven.rdfio.product.EachFile;
import io.github.qudtlib.maven.rdfio.product.Product;
import io.github.qudtlib.maven.rdfio.product.Products;
import io.github.qudtlib.maven.rdfio.product.SingleFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Optional;
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
        for (Product product : products.getProducts()) {
            try {
                if (product instanceof SingleFile) {
                    makeSingleFile((SingleFile) product);
                } else if (product instanceof EachFile) {
                    makeEachFile((EachFile) product);
                }
            } catch (FileNotFoundException e) {
                throw new MojoFailureException(
                        "Error making RDF file for product" + product.describe(), e);
            }
        }
    }

    private void makeSingleFile(SingleFile singleFileProduct)
            throws MojoFailureException, FileNotFoundException, MojoExecutionException {
        singleFileProduct.setLog(getLog());
        getLog().info("Make RDF files configuration:");
        String[] inputFiles = getFilesForPatterns(singleFileProduct.getInput());
        getLog().info("input:");
        Arrays.stream(inputFiles).sorted().forEach(f -> getLog().info("    " + f));
        getLog().info("output: " + singleFileProduct.getOutputFile());
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
        singleFileProduct.process(model);
        writeModelToFile(singleFileProduct.getOutputFile(), model, "writing RDF data to %s");
    }

    private void makeEachFile(EachFile eachFileProduct)
            throws MojoFailureException, FileNotFoundException, MojoExecutionException {
        eachFileProduct.setLog(getLog());
        getLog().info("Make RDF files configuration:");
        String[] inputFiles = getFilesForPatterns(eachFileProduct.getInput());
        getLog().info("input:");
        Arrays.stream(inputFiles).sorted().forEach(f -> getLog().info("    " + f));
        String outputDir =
                Optional.ofNullable(eachFileProduct.getOutputDir()).orElse(getDefaultOutputDir());
        if (eachFileProduct.isReplaceInputFiles()) {
            getLog().info("output: input files are overwritten");
        } else {
            getLog().info("output dir: " + outputDir);
        }
        if (eachFileProduct.isSkip()) {
            getLog().info("Skip making RDF file(s)");
            return;
        }
        debug("Loading data");
        for (String inputFile : inputFiles) {
            Model model = loadRdf(new String[] {inputFile});
            eachFileProduct.process(model);
            if (eachFileProduct.isReplaceInputFiles()) {
                writeModelToFile(inputFile, model, "writing RDF data to %s");
            } else {
                String outputFile = new File(outputDir, new File(inputFile).getName()).toString();
                writeModelToFile(outputFile, model, "writing RDF data to %s");
            }
        }
    }
}
