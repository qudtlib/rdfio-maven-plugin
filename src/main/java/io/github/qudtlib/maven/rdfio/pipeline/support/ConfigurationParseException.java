package io.github.qudtlib.maven.rdfio.pipeline.support;

import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ConfigurationParseException extends RuntimeException {
    private Xpp3Dom configuration;

    public ConfigurationParseException(Xpp3Dom configuration) {
        this.configuration = configuration;
    }

    public ConfigurationParseException(Xpp3Dom configuration, String message) {
        super(message);
        this.configuration = configuration;
    }

    public ConfigurationParseException(Xpp3Dom configuration, String message, Throwable cause) {
        super(message, cause);
        this.configuration = configuration;
    }

    public ConfigurationParseException(Xpp3Dom configuration, Throwable cause) {
        super(cause);
        this.configuration = configuration;
    }

    public ConfigurationParseException(
            Xpp3Dom configuration,
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.configuration = configuration;
    }

    public Xpp3Dom getConfiguration() {
        return configuration;
    }
}
