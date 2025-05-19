package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.topbraid.shacl.rules.RuleUtil;

public class ShaclInferStep implements Step {
    @Parameter private String message;

    @Parameter private Shapes shapes;

    @Parameter private Data data;

    @Parameter private Inferred inferred;

    @Parameter(defaultValue = "false")
    private boolean repeatUntilStable;

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
            if (shapes.getFile() != null) {
                shapes.getFile().forEach(f -> RDFDataMgr.read(shapesModel, f));
            }
            if (shapes.getFiles() != null) {
                Arrays.stream(
                                FileHelper.getFilesForFileSelection(
                                        shapes.getFiles(), state.getBaseDir()))
                        .forEach(f -> RDFDataMgr.read(shapesModel, f));
            }
            if (shapes.getGraph() != null) {
                shapes.getGraph().forEach(g -> shapesModel.add(dataset.getNamedModel(g)));
            }
        }
        Model dataModel = ModelFactory.createDefaultModel();
        if (data != null) {
            if (data.getFile() != null) {
                data.getFile().forEach(f -> RDFDataMgr.read(dataModel, f));
            }
            if (data.getFiles() != null) {
                Arrays.stream(
                                FileHelper.getFilesForFileSelection(
                                        data.getFiles(), state.getBaseDir()))
                        .forEach(f -> RDFDataMgr.read(dataModel, f));
            }
            if (data.getGraph() != null) {
                data.getGraph().forEach(g -> dataModel.add(dataset.getNamedModel(g)));
            }
        }
        if (inferred == null || inferred.getGraph() == null) {
            throw new MojoExecutionException("Inferred graph is required in shaclInfer step");
        }
        Model inferredModel;
        do {
            inferredModel = (Model) RuleUtil.executeRules(dataModel, shapesModel, null, null);
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
    public String calculateHash(String previousHash) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("shaclInfer".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            if (shapes != null) {
                if (shapes.getFile() != null) {
                    shapes.getFile()
                            .forEach(f -> digest.update(f.getBytes(StandardCharsets.UTF_8)));
                }
                if (shapes.getFiles() != null) {
                    shapes.getFiles()
                            .getInclude()
                            .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                    shapes.getFiles()
                            .getExclude()
                            .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
                }
                if (shapes.getGraph() != null) {
                    shapes.getGraph()
                            .forEach(g -> digest.update(g.getBytes(StandardCharsets.UTF_8)));
                }
            }
            if (data != null) {
                if (data.getFile() != null) {
                    data.getFile().forEach(f -> digest.update(f.getBytes(StandardCharsets.UTF_8)));
                }
                if (data.getFiles() != null) {
                    data.getFiles()
                            .getInclude()
                            .forEach(i -> digest.update(i.getBytes(StandardCharsets.UTF_8)));
                    data.getFiles()
                            .getExclude()
                            .forEach(e -> digest.update(e.getBytes(StandardCharsets.UTF_8)));
                }
                if (data.getGraph() != null) {
                    data.getGraph().forEach(g -> digest.update(g.getBytes(StandardCharsets.UTF_8)));
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
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }
}
