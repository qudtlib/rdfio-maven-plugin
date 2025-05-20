package io.github.qudtlib.maven.rdfio.common.file;

import io.github.qudtlib.maven.rdfio.pipeline.ConfigurationParseException;
import java.util.ArrayList;
import java.util.List;
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

    @Parameter
    public void setInclude(String include) {
        this.include.add(include);
    }

    @Parameter
    public void setExclude(String exclude) {
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
        Xpp3Dom[] includeDoms = config.getChildren("include");
        if (includeDoms.length == 0) {
            throw new ConfigurationParseException(
                    """
                            FileSelection requires at least one <include> pattern.
                            Usage: Specify one or more Ant-style include patterns.
                            Example: <include>**/*.ttl</include>""");
        }
        for (Xpp3Dom includeDom : includeDoms) {
            if (includeDom.getValue() != null && !includeDom.getValue().trim().isEmpty()) {
                selection.setInclude(includeDom.getValue().trim());
            } else {
                throw new ConfigurationParseException(
                        """
                                Empty or missing <include> pattern in FileSelection.
                                Usage: Provide a non-empty Ant-style pattern.
                                Example: <include>**/*.ttl</include>""");
            }
        }

        Xpp3Dom[] excludeDoms = config.getChildren("exclude");
        for (Xpp3Dom excludeDom : excludeDoms) {
            if (excludeDom.getValue() != null && !excludeDom.getValue().trim().isEmpty()) {
                selection.setExclude(excludeDom.getValue().trim());
            }
        }

        return selection;
    }
}
