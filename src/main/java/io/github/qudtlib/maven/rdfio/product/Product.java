package io.github.qudtlib.maven.rdfio.product;

import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;

public interface Product {
    void process(Model model) throws MojoExecutionException;

    String describe();
}
