package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

public interface Filter {

    void filter(Dataset dataset) throws MojoExecutionException;

    void setLog(Log log);
}
