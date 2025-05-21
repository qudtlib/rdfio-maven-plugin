package io.github.qudtlib.maven.rdfio.pipeline.step;

import static io.github.qudtlib.maven.rdfio.pipeline.step.AddStep.requireSourceGraphExists;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.InputsComponent;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ShaclFunctionsStep implements Step {

    private InputsComponent<ShaclFunctionsStep> inputsComponent = new InputsComponent<>(this);

    @Override
    public String getElementName() {
        return "shaclFunctions";
    }

    public void setInputsComponent(InputsComponent<ShaclFunctionsStep> inputsComponent) {
        this.inputsComponent = inputsComponent;
    }

    @Override
    public void execute(Dataset dataset, PipelineState state)
            throws RuntimeException, MojoExecutionException {
        Model targetModel = dataset.getNamedModel(state.getShaclFunctionsGraph());
        List<RelativePath> inputPaths = inputsComponent.getAllInputPaths(dataset, state);
        if (!inputPaths.isEmpty()) {
            for (RelativePath inputPath : inputPaths) {
                state.files().readRdf(inputPath, targetModel);
            }
        }
        List<String> inputGraphs = inputsComponent.getAllInputGraphs(dataset, state);
        if (!inputGraphs.isEmpty()) {
            for (String sourceGraph : inputGraphs) {
                requireSourceGraphExists(dataset, sourceGraph);
                Model sourceModel = dataset.getNamedModel(sourceGraph);
                if (sourceModel != null) {
                    targetModel.add(sourceModel);
                }
            }
        }
        SparqlHelper.registerShaclFunctions(
                dataset, state.getShaclFunctionsGraph(), state.getLog());
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("shaclFunctions".getBytes(StandardCharsets.UTF_8));
            inputsComponent.updateHash(digest, state);
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static ShaclFunctionsStep parse(Xpp3Dom config) {
        ShaclFunctionsStep step = new ShaclFunctionsStep();
        ParsingHelper.optionalDomComponent(
                config,
                InputsComponent.getParseFunction(step),
                step::setInputsComponent,
                step::usage);
        return step;
    }

    public String usage() {
        return """
                    Specify inputs using <file>, <files>, <graph> or <graphs>,
                    or nothing to read shacl functions from the default graph.

                    The data is stored in the special graph %s. All functions are made available
                    during SPARQL query/update execution and SHACL processing.
                """
                .formatted(RDFIO.shaclFunctionsGraph.toString());
    }
}
