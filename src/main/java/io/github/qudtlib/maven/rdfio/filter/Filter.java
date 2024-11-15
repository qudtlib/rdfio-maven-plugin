package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.rdf.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

public interface Filter {
    void filter(Model model) throws MojoExecutionException;

    void setLog(Log log);
}
