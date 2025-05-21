package io.github.qudtlib.maven.rdfio.common.file;

import io.github.qudtlib.maven.rdfio.pipeline.support.PipelineConfigurationExeception;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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

    public static boolean isUnderDirectory(File baseDir, File nestedFile) {
        try {
            // Ensure the directory is actually a directory and the file is a file
            if (!baseDir.isDirectory()) {
                return false;
            }

            // Get canonical paths to handle symbolic links and relative paths
            String filePath = null;
            filePath = nestedFile.getCanonicalPath();
            String dirPath = baseDir.getCanonicalPath();

            // Ensure directory path ends with separator for accurate prefix check
            if (!dirPath.endsWith(File.separator)) {
                dirPath += File.separator;
            }

            // Check if file path starts with directory path
            return filePath.startsWith(dirPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void ensureRelativePathsExist(List<RelativePath> files, String kind) {
        ensureFilesExist(files.stream().map(RelativePath::resolve).toList(), kind);
    }

    public static void ensureFilesExist(List<File> files, String kind) {
        for (File file : files) {
            if (!file.exists()) {
                try {
                    throw new PipelineConfigurationExeception(
                            "Configured %s file does not exist: %s"
                                    .formatted(kind, file.getCanonicalPath()));
                } catch (IOException e) {
                    throw new PipelineConfigurationExeception(
                            "Configured %s file does not exist: %s"
                                    .formatted(kind, file.getAbsolutePath()));
                }
            }
        }
    }
}
