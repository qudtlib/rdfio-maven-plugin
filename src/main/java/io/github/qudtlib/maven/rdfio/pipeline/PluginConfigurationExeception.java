package io.github.qudtlib.maven.rdfio.pipeline;

public class PluginConfigurationExeception extends RuntimeException {
    public PluginConfigurationExeception() {}

    public PluginConfigurationExeception(String message) {
        super(message);
    }

    public PluginConfigurationExeception(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginConfigurationExeception(Throwable cause) {
        super(cause);
    }

    public PluginConfigurationExeception(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
