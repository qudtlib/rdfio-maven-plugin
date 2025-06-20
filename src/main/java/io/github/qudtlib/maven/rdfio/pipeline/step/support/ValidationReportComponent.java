package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.step.ShaclValidateStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.util.function.Function;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class ValidationReportComponent implements StepComponent<ShaclValidateStep> {
    private String graph;

    private String file;

    private ShaclValidateStep owner;

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    @Override
    public ShaclValidateStep getOwner() {
        return null;
    }

    public ValidationReportComponent(ShaclValidateStep owner) {
        this.owner = owner;
    }

    public static Function<Xpp3Dom, ValidationReportComponent> getParser(ShaclValidateStep owner) {
        return config -> parse(config, owner);
    }

    // Inferred.java
    public static ValidationReportComponent parse(Xpp3Dom config, ShaclValidateStep owner) {
        ValidationReportComponent component = new ValidationReportComponent(owner);
        if (config == null || config.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    config,
                    """
                            ValidationReport configuration is missing.
                            %s"""
                            .formatted(component.usage()));
        }

        ParsingHelper.optionalStringChild(config, "graph", component::setGraph, component::usage);
        ParsingHelper.optionalStringChild(config, "file", component::setFile, component::usage);
        if (component.getFile() == null && component.getGraph() == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            ValidationReport must have at least one child element.
                            %s"""
                            .formatted(component.usage()));
        }
        return component;
    }

    public String usage() {
        return """
                            Usage: Provide a <validationReport> element with a <graph> and/or <file> element.
                            Example: <validationReport><graph>graph:validationReport</graph></validationReport>""";
    }
}
