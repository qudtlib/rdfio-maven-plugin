package io.github.qudtlib.maven.rdfio.filter;

public enum Graphs {
    EACH("*"),
    DEFAULT("DEFAULT"),
    SHACL_FUNCTIONS_GRAPH("graph:shacl-functions-graph");
    private String pomValue;

    Graphs(String pomValue) {
        this.pomValue = pomValue;
    }

    public String getGraphName() {
        return pomValue;
    }
}
