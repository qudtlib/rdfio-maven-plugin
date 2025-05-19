package io.github.qudtlib.maven.rdfio.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class GraphSelection {
    @Parameter private List<String> include = new ArrayList<>();

    @Parameter private List<String> exclude = new ArrayList<>();

    public List<String> getInclude() {
        return include;
    }

    public void setInclude(String include) {
        this.include.add(include);
    }

    public List<String> getExclude() {
        return exclude;
    }

    public void setExclude(String exclude) {
        this.exclude.add(exclude);
    }
}
