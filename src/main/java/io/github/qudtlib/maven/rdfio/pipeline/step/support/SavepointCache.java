package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import java.util.Objects;
import org.apache.jena.query.Dataset;

public class SavepointCache {
    public static final String HASH_FILE_NAME = "hash.txt";
    public static final String DATASET_FILE_NAME = "dataset.trig";
    private final RelativePath baseDir;

    public SavepointCache(RelativePath baseDir) {
        this.baseDir = baseDir;
    }

    public boolean isValid(String id, String expectedHash, PipelineState state) {
        if (id == null) {
            return false;
        }
        if (expectedHash == null) {
            return false;
        }
        if (state == null) {
            throw new IllegalArgumentException("state cannot be null");
        }
        RelativePath savepointDir = getSavepointDir(id);
        RelativePath hashFile = getHashFile(savepointDir);
        if (!savepointDir.exists() || !hashFile.exists()) {
            return false;
        }
        String storedHash = state.files().readText(hashFile).trim();
        return storedHash.equals(expectedHash);
    }

    public void load(String id, Dataset dataset, PipelineState state) {
        Objects.requireNonNull(id, "cannot load savepoint: id is null");
        Objects.requireNonNull(dataset, "cannot load savepoint into dataset: dataset is null");
        RelativePath datasetFile = getDatasetFile(getSavepointDir(id));
        if (datasetFile.exists()) {
            PipelineHelper.clearDataset(dataset);
            state.files().readRdf(datasetFile, dataset);
        }
    }

    public void save(String id, Dataset dataset, String hash, PipelineState state) {
        Objects.requireNonNull(id, "cannot save savepoint: id is null");
        Objects.requireNonNull(dataset, "cannot save savepoint into dataset: dataset is null");
        Objects.requireNonNull(hash, "cannot save savepoint: hash is null");
        RelativePath savepointDir = getSavepointDir(id);
        state.files().mkdirs(savepointDir);
        RelativePath hashFile = getHashFile(savepointDir);
        RelativePath datasetFile = getDatasetFile(savepointDir);
        state.files().writeRdf(datasetFile, dataset);
        state.files().writeText(hashFile, hash);
    }

    public RelativePath getHashFile(String savepointId) {
        return getHashFile(getSavepointDir(savepointId));
    }

    public RelativePath getDatasetFile(String savepointId) {
        return getDatasetFile(getSavepointDir(savepointId));
    }

    public RelativePath getBaseDir() {
        return this.baseDir;
    }

    public RelativePath getSavepointDir(String savepointId) {
        return baseDir.subDir(savepointId);
    }

    private static RelativePath getHashFile(RelativePath savepointDir) {
        return savepointDir.subFile(HASH_FILE_NAME);
    }

    private static RelativePath getDatasetFile(RelativePath savepointDir) {
        return savepointDir.subFile(DATASET_FILE_NAME);
    }
}
