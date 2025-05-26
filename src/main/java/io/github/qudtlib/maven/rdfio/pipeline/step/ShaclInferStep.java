package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.common.LogHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.FileAccess;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.Inferred;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.InputsComponent;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.topbraid.shacl.rules.RuleUtil;

public class ShaclInferStep implements Step {
    private static final int MAX_INFERENCE_ITERATIONS = 100;
    private String message;

    private InputsComponent<ShaclInferStep> shapes;

    private InputsComponent<ShaclInferStep> data;

    private Inferred inferred;

    private boolean repeatUntilStable = false;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public InputsComponent<ShaclInferStep> getShapes() {
        return shapes;
    }

    public void setShapes(InputsComponent<ShaclInferStep> shapes) {
        this.shapes = shapes;
    }

    public InputsComponent<ShaclInferStep> getData() {
        return data;
    }

    public void setData(InputsComponent<ShaclInferStep> data) {
        this.data = data;
    }

    public Inferred getInferred() {
        return inferred;
    }

    public void setInferred(Inferred inferred) {
        this.inferred = inferred;
    }

    public boolean isRepeatUntilStable() {
        return repeatUntilStable;
    }

    public void setRepeatUntilStable(boolean repeatUntilStable) {
        this.repeatUntilStable = repeatUntilStable;
    }

    public static ShaclInferStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                    ShaclInfer step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        ShaclInferStep step = new ShaclInferStep();

        // Parse optional message
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, ShaclInferStep::usage);
        ParsingHelper.optionalDomChild(
                config,
                "shapes",
                InputsComponent.getParseFunction(step),
                step::setShapes,
                ShaclInferStep::usage);
        ParsingHelper.optionalDomChild(
                config,
                "data",
                InputsComponent.getParseFunction(step),
                step::setData,
                ShaclInferStep::usage);
        ParsingHelper.requiredDomChild(
                config, "inferred", Inferred::parse, step::setInferred, ShaclInferStep::usage);
        ParsingHelper.optionalBooleanChild(
                config, "repeatUntilStable", step::setRepeatUntilStable, ShaclInferStep::usage);

        if (step.getInferred() == null) {
            throw new ConfigurationParseException(
                    config, "<shaclInfer> must have a <inferred> sub-element.\n" + usage());
        }
        if (step.getShapes() == null) {
            throw new ConfigurationParseException(
                    config, "<shaclInfer> must have a <shapes> sub-element.\n" + usage());
        }
        if (step.getData() == null) {
            throw new ConfigurationParseException(
                    config, "<shaclInfer> must have a <data> sub-element.\n" + usage());
        }

        return step;
    }

    public static String usage() {
        return """
                Usage: Specify
                    - <message> (optional): a description of the inference step
                    - <shapes>: <file>, <files>, <graph>, or <graphs> for SHACL shapes (none to use the default graph as the shapes graph)
                    - <data>: data sources via <file>, <files>, <graph>, or <graphs> (none to use the default graph as the data graph)
                    - <inferred>: output via <graph> and/or <file> (none to write inferred triples to the default graph)
                    - <repeatUntilStable> (optional): true to repeat inference until no new triples are added
                    NOTE: shacl functions loaded with <shaclFunctions> can be used in the shapes
                Examples:
                 - <shaclInfer>
                       <message>Inferring triples</message>
                       <shapes><file>shapes.ttl</file></shapes>
                       <data><files><include>data/*.ttl</include></files></data>
                       <inferred><graph>inferred:graph</graph></inferred>
                   </shaclInfer>
                 - <shaclInfer>
                       <shapes><graph>shapes:graph</graph></shapes>
                       <data><graph>data:graph</graph></data>
                       <inferred><file>target/inferred.ttl</file></inferred>
                       <repeatUntilStable>true</repeatUntilStable>
                   </shaclInfer>
                 - <shaclInfer/> - use the default graph for data, shapes and as destination for inferred triples.
               """;
    }

    @Override
    public String getElementName() {
        return "shaclInfer";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (message != null) {
            state.getLog().info(state.variables().resolve(message, dataset));
        }
        try {
            Model shapesModel = populateShapesModel(dataset, state);
            Model dataModel = populateDataModel(dataset, state);
            Model inferredModel = ModelFactory.createDefaultModel();
            Model newTriples = null;
            int i = 0;
            long lastSize = -1;
            boolean modelGrew = false;
            do {
                i++;
                if (i > MAX_INFERENCE_ITERATIONS) {
                    throw new MojoExecutionException(
                            "Limit of %d inference iterations reached without reaching a stable state. More triples keep getting inferred - check your inference rules!"
                                    .formatted(MAX_INFERENCE_ITERATIONS));
                }
                newTriples = RuleUtil.executeRules(dataModel, shapesModel, null, null);
                dataModel.add(newTriples);
                inferredModel.add(newTriples);
                modelGrew = lastSize < inferredModel.size();
                lastSize = inferredModel.size();
            } while (repeatUntilStable && !newTriples.isEmpty() && modelGrew);
            if (inferred != null && inferred.getGraph() != null) {
                dataset.addNamedModel(inferred.getGraph(), inferredModel);
                PipelineHelper.bindGraphToNoFileIfUnbound(dataset, state, inferred.getGraph());
            } else {
                dataset.getDefaultModel().add(inferredModel);
            }
            if (inferred != null && inferred.getFile() != null) {
                RelativePath path = state.files().make(inferred.getFile());
                state.files().writeRdf(path, inferredModel);
            }
            state.getPrecedingSteps().add(this);
        } catch (RuntimeException e) {
            throw new MojoExecutionException(
                    "Error executing ShaclInferStep\nProblem: %s\n%s"
                            .formatted(e.getMessage(), usage()),
                    e);
        }
    }

    private Model populateDataModel(Dataset dataset, PipelineState state)
            throws MojoExecutionException {
        return populateModelFromInputs(dataset, state, this.data, List.of(), "SHACL data");
    }

    private Model populateShapesModel(Dataset dataset, PipelineState state) {
        Model shapesModel =
                populateModelFromInputs(
                        dataset,
                        state,
                        this.shapes,
                        List.of(state.getShaclFunctionsGraph()),
                        "SHACL shapes");
        return shapesModel;
    }

    private static Model populateModelFromInputs(
            Dataset dataset,
            PipelineState state,
            InputsComponent<ShaclInferStep> inputsComponent,
            List<String> additionalGraphs,
            String fileKind) {
        Model dataModel = ModelFactory.createDefaultModel();
        List<String> entries = new ArrayList<>();
        String indent = "        ";
        if (inputsComponent == null || inputsComponent.hasNoInputs()) {
            dataModel.add(dataset.getDefaultModel());
            entries.add(PipelineHelper.formatDefaultGraph(indent));
        } else {
            List<RelativePath> dataPaths = inputsComponent.getAllInputPaths(dataset, state);
            FileHelper.ensureRelativePathsExist(dataPaths, fileKind);
            FileAccess.readRdf(dataPaths, dataModel, state);
            entries.addAll(PipelineHelper.formatPaths(dataPaths, indent));
            List<String> allGraphs = new ArrayList<>();
            allGraphs.addAll(additionalGraphs);
            allGraphs.addAll(inputsComponent.getAllInputGraphs(dataset, state));
            if (allGraphs != null) {
                allGraphs.forEach(g -> dataModel.add(dataset.getNamedModel(g)));
            }
            entries.addAll(PipelineHelper.formatGraphs(allGraphs, indent));
        }
        state.getLog().info("    " + fileKind);
        LogHelper.info(state.getLog(), entries);
        return dataModel;
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("shaclInfer".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            if (shapes != null) {
                shapes.updateHash(digest, state);
            }
            if (data != null) {
                data.updateHash(digest, state);
            }
            if (inferred != null) {
                if (inferred.getGraph() != null) {
                    digest.update(inferred.getGraph().getBytes(StandardCharsets.UTF_8));
                }
                if (inferred.getFile() != null) {
                    digest.update(inferred.getFile().getBytes(StandardCharsets.UTF_8));
                }
            }
            digest.update(String.valueOf(repeatUntilStable).getBytes(StandardCharsets.UTF_8));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
