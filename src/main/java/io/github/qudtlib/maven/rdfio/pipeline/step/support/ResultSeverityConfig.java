package io.github.qudtlib.maven.rdfio.pipeline.step.support;

public enum ResultSeverityConfig {
    Info,
    Warning,
    Violation,
    None;

    public boolean isHigherThanOrEqualTo(ResultSeverityConfig other) {
        if (this == None || other == None) {
            return false;
        }
        switch (this) {
            case Violation:
                return true;
            case Warning:
                return other != Violation;
            case Info:
                return other != Violation && other != Warning;
        }
        return false;
    }
}
