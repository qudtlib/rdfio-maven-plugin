package io.github.qudtlib.maven.rdfio.common.file;

import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class FileSelection {

    /**
     * Comma-separated or newline-separated list of ant-style patterns, such as <code>
     * target/txt/**\/*.txt,target/pdf/**\/*.pdf</code>
     */
    protected List<String> include = new ArrayList<>();

    /**
     * Comma-separated or newline-separated list of ant-style patterns, such as <code>
     * target/txt/**\/*.txt,target/pdf/**\/*.pdf</code>
     */
    protected List<String> exclude = new ArrayList<>();

    public FileSelection() {}

    public FileSelection(List<String> include, List<String> exclude) {
        this.include = include;
        this.exclude = exclude;
    }

    @Parameter
    public void addInclude(String include) {
        this.include.add(include);
    }

    @Parameter
    public void addExclude(String exclude) {
        this.exclude.add(exclude);
    }

    public List<String> getInclude() {
        return include;
    }

    public List<String> getExclude() {
        return exclude;
    }

    // FileSelection.java
    public static FileSelection parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config,
                    """
                            FileSelection configuration is missing.
                            Usage: Provide a <files> element with at least one <include> pattern.
                            Example:
                            <files>
                                <include>**/*.ttl</include>
                                <exclude>**/temp/*.ttl</exclude>
                            </files>""");
        }

        FileSelection selection = new FileSelection();
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

    public static String usage() {
        return """
                 Usage: Provide a <files> element with at least one <include> pattern.
                            Example:
                            <files>
                                <include>**/*.ttl</include>
                                <exclude>**/temp/*.ttl</exclude>
                            </files>""";
    }

    public boolean isEmpty() {
        return this.include.isEmpty() && this.exclude.isEmpty();
    }
}
