package io.github.qudtlib.maven.rdfio.pipeline;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

public class SavepointStep implements Step {
    @Parameter private String id;

    @Parameter(defaultValue = "true")
    private boolean enabled;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isValid(Dataset dataset, PipelineState state, String currentHash)
            throws MojoExecutionException {
        if (!enabled) {
            return false;
        }
        if (id == null) {
            throw new MojoExecutionException("Savepoint id is required");
        }
        SavepointCache cache = state.getSavepointCache();
        return cache.isValid(id, currentHash, state.getPipelineId());
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (id == null) {
            throw new MojoExecutionException("Savepoint id is required");
        }
        if (!enabled) {
            state.getPrecedingSteps().add(this);
            return;
        }
        SavepointCache cache = state.getSavepointCache();
        String currentHash = calculateHash(state.getPreviousStepHash());
        if (state.isAllowLoadingFromSavepoint()
                && cache.isValid(id, currentHash, state.getPipelineId())) {
            cache.load(id, dataset);
            state.setAllowLoadingFromSavepoint(false);
        } else {
            cache.save(id, dataset, currentHash);
            state.getPrecedingSteps().add(this);
        }
    }

    @Override
    public String calculateHash(String previousHash) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("savepoint".getBytes(StandardCharsets.UTF_8));
            if (id != null) {
                digest.update(id.getBytes(StandardCharsets.UTF_8));
            }
            digest.update(String.valueOf(enabled).getBytes(StandardCharsets.UTF_8));
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
