package io.github.qudtlib.maven.rdfio.common.file;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

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
}
