package io.github.qudtlib.maven.rdfio.common.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.tools.ant.DirectoryScanner;

public class FileHelper {
    public static String[] splitPatterns(String patterns) {
        return Arrays.stream(patterns.split("(\\s|\n)*([,\n])(\\s|\n)*"))
                .map(String::trim)
                .toArray(String[]::new);
    }

    public static String[] getFilesForFileSelection(FileSelection fileSelection, File basedir) {
        String[] includes =
                fileSelection.getInclude().stream()
                        .map(FileHelper::splitPatterns)
                        .flatMap(Arrays::stream)
                        .toList()
                        .toArray(String[]::new);
        String[] excludes =
                fileSelection.getExclude().stream()
                        .map(FileHelper::splitPatterns)
                        .flatMap(Arrays::stream)
                        .toList()
                        .toArray(String[]::new);
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setBasedir(basedir);
        scanner.setIncludes(includes);
        scanner.setExcludes(excludes);
        scanner.scan();
        return scanner.getIncludedFiles();
    }

    public static String relativizeAsUnixStyle(File baseDir, File nestedFile) {
        return baseDir.toPath()
                .relativize(nestedFile.toPath())
                .toFile()
                .toString()
                .replace("\\", "/");
    }

    public static File resolveRelativeUnixPath(File baseDir, String unixStylePath) {
        try {
            return baseDir.toPath().resolve(Path.of(unixStylePath)).toFile().getCanonicalFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Resource getFileUrl(File file) {
        try {
            return ResourceFactory.createResource(file.getCanonicalFile().toURI().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
