package io.github.qudtlib.maven.rdfio.filter;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class IncludeExcludePatterns {
    /**
     * Comma-separated or newline-separated list of ant-style patterns, such as <code>
     * target/txt/**\/*.txt,target/pdf/**\/*.pdf</code>
     */
    @Parameter private List<String> include = new ArrayList<>();

    /**
     * Comma-separated or newline-separated list of ant-style patterns, such as <code>
     * target/txt/**\/*.txt,target/pdf/**\/*.pdf</code>
     */
    @Parameter private List<String> exclude = new ArrayList<>();

    public void setInclude(String include) {
        this.include.add(include);
    }

    public void setExclude(String exclude) {
        this.exclude.add(exclude);
    }

    public List<String> getInclude() {
        return include;
    }

    public List<String> getExclude() {
        return exclude;
    }

    @Override
    public String toString() {
        return "IncludeExcludePatterns{"
                + "include='"
                + include
                + '\''
                + ", exclude='"
                + exclude
                + '\''
                + '}';
    }
}
