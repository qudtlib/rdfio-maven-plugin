package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class SavepointCache {
    private final File baseDir;

    public SavepointCache(File baseDir, String pipelineId) {
        Objects.requireNonNull(pipelineId, "Cannot create savepoint cache: pipelineId is null");
        this.baseDir =
                baseDir.toPath()
                        .resolve(Path.of("rdfio/pipeline/" + pipelineId + "/savepoint"))
                        .toFile();
    }

    public boolean isValid(String id, String expectedHash, String pipelineId) {
        if (id == null) {
            return false;
        }
        if (expectedHash == null) {
            return false;
        }
        if (pipelineId == null) {
            return false;
        }
        File savepointDir = new File(baseDir, id);
        File hashFile = new File(savepointDir, "hash.txt");
        if (!savepointDir.exists() || !hashFile.exists()) {
            return false;
        }
        try {
            String storedHash = Files.readString(hashFile.toPath(), StandardCharsets.UTF_8).trim();
            return storedHash.equals(expectedHash);
        } catch (IOException e) {
            return false;
        }
    }

    public void load(String id, Dataset dataset) {
        Objects.requireNonNull(id, "cannot load savepoint: id is null");
        Objects.requireNonNull(dataset, "cannot load savepoint into dataset: dataset is null");
        File datasetFile = new File(getSavepointDir(id), "dataset.trig");
        if (datasetFile.exists()) {
            PipelineHelper.clearDataset(dataset);
            try {
                RDFDataMgr.read(dataset, new FileInputStream(datasetFile), Lang.TRIG);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Error reading savepoint file " + datasetFile, e);
            }
        }
    }

    public void save(String id, Dataset dataset, String hash) {
        Objects.requireNonNull(id, "cannot save savepoint: id is null");
        Objects.requireNonNull(dataset, "cannot save savepoint into dataset: dataset is null");
        Objects.requireNonNull(hash, "cannot save savepoint: hash is null");
        File savepointDir = new File(baseDir, id);
        savepointDir.mkdirs();
        File hashFile = new File(savepointDir, "hash.txt");
        File datasetFile = new File(savepointDir, "dataset.trig");
        try {
            Files.write(hashFile.toPath(), hash.getBytes(StandardCharsets.UTF_8));
            RDFDataMgr.write(new FileOutputStream(datasetFile), dataset, Lang.TRIG);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save savepoint: " + id, e);
        }
    }

    public File getBaseDir() {
        return this.baseDir;
    }

    public File getSavepointDir(String savepointId) {
        Objects.requireNonNull(savepointId, "Cannot make savepoint dir name: savepointId is null");
        return new File(this.baseDir, savepointId);
    }
}
