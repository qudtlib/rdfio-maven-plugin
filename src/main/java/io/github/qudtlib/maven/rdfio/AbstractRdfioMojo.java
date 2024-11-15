package io.github.qudtlib.maven.rdfio;

import io.github.qudtlib.maven.rdfio.filter.IncludeExcludePatterns;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.tools.ant.DirectoryScanner;

public abstract class AbstractRdfioMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project.basedir}", readonly = true)
    protected File basedir;

    static String[] splitPatterns(String patterns) {
        return Arrays.stream(patterns.split("(\\s|\n)*([,\n])(\\s|\n)*"))
                .map(String::trim)
                .toArray(String[]::new);
    }

    protected void writeModelToFile(String outputFile, Model model, String messageFormat)
            throws FileNotFoundException {
        if (outputFile != null) {
            File folder = new File(outputFile).getParentFile();
            if (!folder.exists()) {
                folder.mkdirs();
            }
            deleteCarriageReturns(model);
            RDFDataMgr.write(
                    new FileOutputStream(new File(basedir, outputFile)),
                    model,
                    RDFLanguages.resourceNameToLang(outputFile, Lang.TTL));
            getLog().info(String.format(messageFormat, outputFile));
        }
    }

    private static void deleteCarriageReturns(Model model) {
        Pattern containsR = Pattern.compile("\r", Pattern.MULTILINE);
        List<Statement> newStatements = new ArrayList<>();
        StmtIterator it = model.listStatements();
        while (it.hasNext()) {
            Statement s = it.nextStatement();
            RDFNode object = s.getObject();
            if (object.isLiteral()) {
                String stringValue;
                try {
                    stringValue = object.asLiteral().getString();
                    RDFDatatype rdfDatatype = object.asLiteral().getDatatype();
                    Matcher m = containsR.matcher(stringValue);
                    if (m.find()) {
                        stringValue = m.replaceAll("");
                        RDFNode newObject =
                                rdfDatatype == null
                                        ? ResourceFactory.createStringLiteral(stringValue)
                                        : ResourceFactory.createTypedLiteral(
                                                stringValue, rdfDatatype);
                        Statement newStatement =
                                new StatementImpl(s.getSubject(), s.getPredicate(), newObject);
                        newStatements.add(newStatement);
                        it.remove();
                    }
                } catch (Node.NotLiteral e) {
                    // that's ok - value is not a string
                }
            }
        }
        model.add(newStatements);
    }

    protected Model loadRdf(String[] files) throws MojoExecutionException {
        Model model = ModelFactory.createDefaultModel();

        for (String file : files) {
            debug("Loading %s", file);
            File inFile = new File(basedir, file);
            Lang lang = RDFLanguages.resourceNameToLang(inFile.getName(), Lang.TTL);
            try {
                String content = Files.readString(inFile.toPath());
                ByteArrayInputStream inputStream =
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
                RDFParser.source(inputStream).lang(lang).parse(model.getGraph());
            } catch (IOException e) {
                throw new MojoExecutionException(
                        "Error parsing RDF file " + inFile.getAbsolutePath(), e);
            }
        }
        debug("Loaded %d triples", model.size());
        return model;
    }

    protected void debug(String pattern, Object... args) {
        if (getLog().isDebugEnabled()) {
            getLog().debug(String.format(pattern, args));
        }
    }

    protected String[] getFilesForPatterns(IncludeExcludePatterns includeExcludePatterns) {
        String[] includes = AbstractRdfioMojo.splitPatterns(includeExcludePatterns.getInclude());
        String[] excludes = AbstractRdfioMojo.splitPatterns(includeExcludePatterns.getExclude());
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setBasedir(basedir);
        scanner.setIncludes(includes);
        scanner.setExcludes(excludes);
        scanner.scan();
        return scanner.getIncludedFiles();
    }
}
