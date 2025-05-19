package io.github.qudtlib.maven.rdfio.common.file;

import java.io.File;
import java.util.Arrays;
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
}
