package io.github.qudtlib.maven.rdfio.filter;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;

public class SparqlQueryFileFilter extends SparqlQueryFilter {
    private final String filename;
    private final File basedir;

    public SparqlQueryFileFilter(String filename, File basedir) throws IOException {
        super(null);
        this.filename = filename;
        this.basedir = basedir;
    }

    @Override
    protected String getSparqlSelectString() throws MojoExecutionException {
        File sparqlSelectFile = new File(this.basedir, this.filename);
        try {
            return FileUtils.readFileToString(sparqlSelectFile);
        } catch (IOException e) {
            throw new MojoExecutionException(
                    String.format(
                            "Cannot read SPARQL Select query from file %s: ", sparqlSelectFile),
                    e);
        }
    }
}
