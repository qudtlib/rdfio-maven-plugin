package io.github.qudtlib.maven.rdfio.filter;

public interface OutputToGraphFilter extends Filter {

    String getOutputGraph();

    /**
     * If true, any triples exising in the output graph are deleted before the output of this filter
     * is added.
     *
     * @return
     */
    boolean isClearBeforeInsert();
}
