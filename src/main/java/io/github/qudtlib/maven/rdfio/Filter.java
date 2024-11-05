package io.github.qudtlib.maven.rdfio;

import org.apache.jena.rdf.model.Model;

public interface Filter {
    void filter(Model model);
}
