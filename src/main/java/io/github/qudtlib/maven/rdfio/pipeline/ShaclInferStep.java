package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.topbraid.shacl.rules.RuleUtil;

public class ShaclInferStep implements Step {
    private static final int MAX_INFERENCE_ITERATIONS = 100;
    private String message;

    private Shapes shapes;

    private Data data;

    private Inferred inferred;

    private boolean repeatUntilStable = false;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Shapes getShapes() {
        return shapes;
    }

    public void setShapes(Shapes shapes) {
        this.shapes = shapes;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
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
                    """
                    ShaclInfer step configuration is missing.
                    %s"""
                            .formatted(usage()));
        }

        ShaclInferStep step = new ShaclInferStep();

        // Parse optional message
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, ShaclInferStep::usage);
        ParsingHelper.requiredDomChild(
                config, "shapes", Shapes::parse, step::setShapes, ShaclInferStep::usage);
        ParsingHelper.requiredDomChild(
                config, "data", Data::parse, step::setData, ShaclInferStep::usage);
        ParsingHelper.requiredDomChild(
                config, "inferred", Inferred::parse, step::setInferred, ShaclInferStep::usage);
        ParsingHelper.optionalBooleanChild(
                config, "repeatUntilStable", step::setRepeatUntilStable, ShaclInferStep::usage);

        if (step.getInferred() == null) {
            throw new ConfigurationParseException(
                    "<shaclInfer> must have a <inferred> sub-element.\n" + usage());
        }
        if (step.getShapes() == null) {
            throw new ConfigurationParseException(
                    "<shaclInfer> must have a <shapes> sub-element.\n" + usage());
        }
        if (step.getData() == null) {
            throw new ConfigurationParseException(
                    "<shaclInfer> must have a <data> sub-element.\n" + usage());
        }

        return step;
    }

    public static String usage() {
        return """
                Usage: Specify
                    - <message> (optional): a description of the inference step
                    - <shapes>: at least one of <file>, <files>, <graph>, or <graphs> for SHACL shapes
                    - <data>: data sources via <file>, <files>, <graph>, or <graphs>
                    - <inferred>: output via <graph> and/or <file>
                    - <repeatUntilStable> (optional): true to repeat inference until no new triples are added
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
               """;
    }

    @Override
    public String getElementName() {
        return "shaclInfer";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        try {
            Model shapesModel = ModelFactory.createDefaultModel();
            if (shapes != null) {
                List<File> shapesFiles =
                        RdfFileProcessor.resolveFiles(
                                shapes.getFiles(), shapes.getFileSelection(), state.getBaseDir());
                FileHelper.ensureFilesExist(shapesFiles, "shapes");
                RdfFileProcessor.loadRdfFiles(shapesFiles, shapesModel);
                if (shapes.getGraphs() != null) {
                    PipelineHelper.ensureGraphsExist(dataset, data.getGraphs(), "shapes");
                    shapes.getGraphs().forEach(g -> shapesModel.add(dataset.getNamedModel(g)));
                }
                PipelineHelper.getGraphs(dataset, shapes.getGraphSelection())
                        .forEach(g -> shapesModel.add(dataset.getNamedModel(g)));
            }
            Model dataModel = ModelFactory.createDefaultModel();
            if (data != null) {
                List<File> files =
                        RdfFileProcessor.resolveFiles(
                                data.getFiles(), data.getFileSelection(), state.getBaseDir());
                FileHelper.ensureFilesExist(files, "data");
                RdfFileProcessor.loadRdfFiles(files, dataModel);
                if (data.getGraphs() != null) {
                    PipelineHelper.ensureGraphsExist(dataset, data.getGraphs(), "data");
                    data.getGraphs().forEach(g -> dataModel.add(dataset.getNamedModel(g)));
                }
                PipelineHelper.getGraphs(dataset, data.getGraphSelection())
                        .forEach(g -> dataModel.add(dataset.getNamedModel(g)));
            }
            if (inferred == null || inferred.getGraph() == null) {
                throw new MojoExecutionException("Inferred graph is required in shaclInfer step");
            }
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
            dataset.addNamedModel(inferred.getGraph(), inferredModel);
            if (inferred.getFile() != null) {
                try (FileOutputStream out = new FileOutputStream(inferred.getFile())) {
                    RDFDataMgr.write(out, inferredModel, Lang.TTL);
                } catch (Exception e) {
                    throw new MojoExecutionException("Failed to write inferred file", e);
                }
            }
            state.getPrecedingSteps().add(this);
        } catch (RuntimeException e) {
            throw new MojoExecutionException(
                    "Error executing ShaclInferStep\n%s".formatted(usage()), e);
        }
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
                RdfFileProcessor.updateHashWithFiles(
                        RdfFileProcessor.resolveFiles(
                                shapes.getFiles(), shapes.getFileSelection(), state.getBaseDir()),
                        digest);
                if (shapes.getFileSelection() != null) {
                    shapes.getFileSelection()
                            .getInclude()
                            .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                    shapes.getFileSelection()
                            .getExclude()
                            .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
                }
                if (shapes.getGraphs() != null) {
                    shapes.getGraphs()
                            .forEach(g -> digest.update(g.getBytes(StandardCharsets.UTF_8)));
                }
            }
            if (data != null) {
                RdfFileProcessor.updateHashWithFiles(
                        RdfFileProcessor.resolveFiles(
                                data.getFiles(), data.getFileSelection(), state.getBaseDir()),
                        digest);
                if (data.getFileSelection() != null) {
                    data.getFileSelection()
                            .getInclude()
                            .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                    data.getFileSelection()
                            .getExclude()
                            .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
                }
                if (data.getGraphs() != null) {
                    data.getGraphs()
                            .forEach(g -> digest.update(g.getBytes(StandardCharsets.UTF_8)));
                }
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
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
