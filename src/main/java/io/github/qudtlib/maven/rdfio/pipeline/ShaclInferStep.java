package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.topbraid.shacl.rules.RuleUtil;

public class ShaclInferStep implements Step {
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

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        Model shapesModel = ModelFactory.createDefaultModel();
        if (shapes != null) {
            RdfFileProcessor.loadRdfFiles(
                    RdfFileProcessor.resolveFiles(
                            shapes.getFiles(), shapes.getFileSelection(), state.getBaseDir()),
                    shapesModel);
            if (shapes.getGraphs() != null) {
                shapes.getGraphs().forEach(g -> shapesModel.add(dataset.getNamedModel(g)));
            }
            PipelineHelper.getGraphs(dataset, shapes.getGraphSelection())
                    .forEach(g -> shapesModel.add(dataset.getNamedModel(g)));
        }
        Model dataModel = ModelFactory.createDefaultModel();
        if (data != null) {
            RdfFileProcessor.loadRdfFiles(
                    RdfFileProcessor.resolveFiles(
                            data.getFiles(), data.getFileSelection(), state.getBaseDir()),
                    dataModel);
            if (data.getGraphs() != null) {
                data.getGraphs().forEach(g -> dataModel.add(dataset.getNamedModel(g)));
            }
            PipelineHelper.getGraphs(dataset, data.getGraphSelection())
                    .forEach(g -> dataModel.add(dataset.getNamedModel(g)));
        }
        if (inferred == null || inferred.getGraph() == null) {
            throw new MojoExecutionException("Inferred graph is required in shaclInfer step");
        }
        Model inferredModel;
        do {
            inferredModel = RuleUtil.executeRules(dataModel, shapesModel, null, null);
            dataModel.add(inferredModel);
        } while (repeatUntilStable && !inferredModel.isEmpty());
        dataset.addNamedModel(inferred.getGraph(), inferredModel);
        if (inferred.getFile() != null) {
            try (FileOutputStream out = new FileOutputStream(inferred.getFile())) {
                RDFDataMgr.write(out, inferredModel, Lang.TTL);
            } catch (Exception e) {
                throw new MojoExecutionException("Failed to write inferred file", e);
            }
        }
        state.getPrecedingSteps().add(this);
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

    // ShaclInferStep.java
    public static ShaclInferStep parse(Xpp3Dom config) throws MojoExecutionException {
        if (config == null) {
            throw new MojoExecutionException(
                    """
                            ShaclInfer step configuration is missing.
                            Usage: Provide a <shaclInfer> element with <shapes>, <data>, and <inferred>.
                            Example:
                            <shaclInfer>
                                <shapes><file>shapes.ttl</file></shapes>
                                <data><file>data.ttl</file></data>
                                <inferred><graph>inferred:graph</graph></inferred>
                            </shaclInfer>""");
        }

        ShaclInferStep step = new ShaclInferStep();
        String message = ParsingHelper.getNonBlankChildString(config, "message");
        step.setMessage(message);
        Xpp3Dom shapesDom = config.getChild("shapes");
        if (shapesDom == null) {
            throw new MojoExecutionException(
                    """
                            ShaclInfer step requires a <shapes> element.
                            Usage: Specify SHACL shapes via <file> or <graph>.
                            Example: <shapes><file>shapes.ttl</file></shapes>""");
        }
        step.setShapes(Shapes.parse(shapesDom));

        Xpp3Dom dataDom = config.getChild("data");
        step.setData(dataDom != null ? Data.parse(dataDom) : new Data());

        Xpp3Dom inferredDom = config.getChild("inferred");
        if (inferredDom == null) {
            throw new MojoExecutionException(
                    """
                            ShaclInfer step requires an <inferred> element.
                            Usage: Specify the target graph for inferred triples.
                            Example: <inferred><graph>inferred:graph</graph></inferred>""");
        }
        step.setInferred(Inferred.parse(inferredDom));

        return step;
    }
}
