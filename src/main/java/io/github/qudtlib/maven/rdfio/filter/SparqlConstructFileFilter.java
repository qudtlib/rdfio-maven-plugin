package io.github.qudtlib.maven.rdfio.filter;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlConstructFileFilter extends SparqlConstructFilter {
    private final File basedir;

    public SparqlConstructFileFilter(String filename, File basedir) throws IOException {
        super(filename);
        this.basedir = basedir;
    }

    @Override
    protected String getSparqlConstructString() throws MojoExecutionException {
        File sparqlConstructFile = new File(this.basedir, this.constructQuery);
        try {
            return FileUtils.readFileToString(sparqlConstructFile);
        } catch (IOException e) {
            throw new MojoExecutionException(
                    String.format(
                            "Cannot read SPARQL Construct query from file %s: ",
                            this.constructQuery),
                    e);
        }
    }
}
