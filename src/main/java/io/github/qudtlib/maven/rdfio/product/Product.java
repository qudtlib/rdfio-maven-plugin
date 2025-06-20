package io.github.qudtlib.maven.rdfio.product;

import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;

public interface Product {
    void process(Dataset dataset) throws MojoExecutionException;

    String describe();
}
