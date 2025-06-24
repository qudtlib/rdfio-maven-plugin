package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ClearStep implements Step {

    @Override
    public String getElementName() {
        return "clear";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state)
            throws RuntimeException, MojoExecutionException {
        state.log().info("Clearing dataset");
        dataset.getDefaultModel().removeAll();
        Iterator<String> nameIt = dataset.listNames();
        while (nameIt.hasNext()) {
            dataset.removeNamedModel(nameIt.next());
        }
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("clear".getBytes(StandardCharsets.UTF_8));
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static ClearStep parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            Write step configuration is missing.
                            %s"""
                            .formatted(WriteStep.usage()));
        }
        return new ClearStep();
    }

    public static String usage() {
        return """
                           Usage:

                           Clears the dataset. Nothing remains, no shaclFunctions, no variables, no metadata.

                           Example:
                           <clear/>""";
    }
}
