package io.github.qudtlib.maven.rdfio.filter;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlUpdateFileFilter extends SparqlUpdateFilter {
    private final File basedir;

    public SparqlUpdateFileFilter(String filename, File basedir) throws IOException {
        super(filename);
        this.basedir = basedir;
    }

    @Override
    protected String getSparqlUpdateString() throws MojoExecutionException {
        File sparqlUpdateFile = new File(this.basedir, this.update);
        try {
            return FileUtils.readFileToString(sparqlUpdateFile);
        } catch (IOException e) {
            throw new MojoExecutionException(
                    String.format("Cannot read SPARQL update from file %s: ", this.update), e);
        }
    }
}
