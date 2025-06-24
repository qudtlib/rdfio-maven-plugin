package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class GraphSelection {
    private final List<String> include = new ArrayList<>();

    private final List<String> exclude = new ArrayList<>();

    public GraphSelection() {}

    public GraphSelection(List<String> include, List<String> exclude) {
        this.include.addAll(include);
        this.exclude.addAll(exclude);
    }

    public List<String> getInclude() {
        return include;
    }

    public void addInclude(String include) {
        this.include.add(include);
    }

    public List<String> getExclude() {
        return exclude;
    }

    public void addExclude(String exclude) {
        this.exclude.add(exclude);
    }

    // GraphSelection.java
    public static GraphSelection parse(Xpp3Dom config) throws ConfigurationParseException {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            GraphSelection configuration is missing.
                            %s"""
                            .formatted(usage()));
        }

        GraphSelection selection = new GraphSelection();
        ParsingHelper.requiredStringChildren(
                config, "include", selection::splitAndAddInclude, FileSelection::usage);
        ParsingHelper.optionalStringChildren(
                config, "exclude", selection::splitAndAddExclude, FileSelection::usage);
        return selection;
    }

    private void splitAndAddInclude(String includeStringValue) {
        splitAndAdd(includeStringValue, this::addInclude);
    }

    private void splitAndAddExclude(String excludeStringValue) {
        splitAndAdd(excludeStringValue, this::addExclude);
    }

    private void splitAndAdd(String stringValue, Consumer<String> setter) {
        String[] splitValues = stringValue.split("\\s*(,|\\n|,\\s*\\n|\\n\\s*,)\\s*");
        for (int i = 0; i < splitValues.length; i++) {
            setter.accept(splitValues[i]);
        }
    }

    public boolean isEmpty() {
        return this.include.isEmpty() && this.exclude.isEmpty();
    }

    public static String usage() {
        return """
                 Usage: Provide a <graphs> element with at least one <include> pattern.
                            Example:
                            <graphs>
                                <include>inferred:*</include>
                                <exclude>inferred:thatOneGraph</exclude>
                            </graphs>""";
    }

    public void updateHash(MessageDigest digest, PipelineState state) {
        this.include.forEach(s -> digest.update(s.getBytes(StandardCharsets.UTF_8)));
        this.exclude.forEach(s -> digest.update(s.getBytes(StandardCharsets.UTF_8)));
    }
}
