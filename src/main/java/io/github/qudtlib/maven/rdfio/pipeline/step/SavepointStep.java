package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.SavepointCache;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class SavepointStep implements Step {
    private String id;

    private boolean enabled = true;

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

    public boolean isValid(PipelineState state, String currentHash) throws MojoExecutionException {
        if (!enabled) {
            return false;
        }
        if (id == null) {
            throw new MojoExecutionException("Savepoint id is required");
        }
        SavepointCache cache = state.getSavepointCache();
        return cache.isValid(id, currentHash, state);
    }

    @Override
    public String getElementName() {
        return "savepoint";
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
        String currentHash = calculateHash(state.getPreviousStepHash(), state);
        if (cache.isValid(id, currentHash, state)) {
            if (state.isAllowLoadingFromSavepoint()) {
                cache.load(id, dataset, state);
                state.setAllowLoadingFromSavepoint(false);
            }
        } else {
            cache.save(id, dataset, currentHash, state);
            state.getPrecedingSteps().add(this);
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("savepoint".getBytes(StandardCharsets.UTF_8));
            if (id != null) {
                digest.update(id.getBytes(StandardCharsets.UTF_8));
            }
            digest.update(String.valueOf(enabled).getBytes(StandardCharsets.UTF_8));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    // SavepointStep.java
    public static SavepointStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Savepoint step configuration is missing.
                            Usage: Provide a <savepoint> element with a required <id> and optional <enabled>.
                            Example:
                            <savepoint>
                                <id>sp001</id>
                                <enabled>true</enabled>
                            </savepoint>""");
        }

        SavepointStep step = new SavepointStep();
        Xpp3Dom idDom = config.getChild("id");
        if (idDom == null || idDom.getValue() == null || idDom.getValue().trim().isEmpty()) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Savepoint step requires a non-empty <id>.
                            Usage: Specify a unique identifier for the savepoint.
                            Example: <id>sp001</id>""");
        }
        step.setId(idDom.getValue().trim());

        Xpp3Dom enabledDom = config.getChild("enabled");
        step.setEnabled(
                enabledDom == null
                        || enabledDom.getValue() == null
                        || Boolean.parseBoolean(enabledDom.getValue().trim()));

        return step;
    }
}
