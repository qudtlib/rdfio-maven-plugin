package io.github.qudtlib.maven.rdfio.pipeline;

public class PipelineConfigurationExeception extends RuntimeException {
    public PipelineConfigurationExeception() {}

    public PipelineConfigurationExeception(String message) {
        super(message);
    }

    public PipelineConfigurationExeception(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineConfigurationExeception(Throwable cause) {
        super(cause);
    }

    public PipelineConfigurationExeception(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
