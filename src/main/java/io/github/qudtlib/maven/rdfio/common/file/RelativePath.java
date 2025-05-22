package io.github.qudtlib.maven.rdfio.common.file;

import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.File;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class RelativePath {
    private static final Pattern TRAVERSAL_PATTERN = Pattern.compile("(^|/)\\.\\.(/|$)");
    private static final Pattern ABSOLUTE_PATH_PATTERN = Pattern.compile("^(/|\\\\|[a-zA-Z]:\\\\)");

    private final File baseDir;
    private final String relativePath;
    private File resolved = null;

    public RelativePath(File baseDir, String relativePath) {
        Objects.requireNonNull(baseDir, "baseDir must not be null");
        Objects.requireNonNull(relativePath, "relativePath must not be null");
        if (relativePath.trim().isEmpty()) {
            throw new RelativePathException("relativePath must not be empty");
        }
        if (TRAVERSAL_PATTERN.matcher(relativePath).find()) {
            throw new RelativePathException(
                    "relativePath must not contain '..' to prevent directory traversal: "
                            + relativePath);
        }
        if (ABSOLUTE_PATH_PATTERN.matcher(relativePath).find()) {
            throw new RelativePathException(
                    "Absolute paths are not allowed in configuration: " + relativePath);
        }
        this.baseDir = baseDir;
        // Normalize to Unix-style separators
        this.relativePath = relativePath.replace("\\", "/").replaceAll("/+", "/").trim();
    }

    public static RelativePath of(File baseDir, File file) {
        if (file == null || file.toString().trim().isEmpty()) {
            throw new RelativePathException("Path must not be null or empty");
        }
        if (file.isAbsolute()) {
            if (FileHelper.isUnderDirectory(baseDir, file)) {
                String relative =
                        file.toPath()
                                .relativize(baseDir.toPath())
                                .toString()
                                .replaceFirst("^\\.", "")
                                .replace("\\", "/");

                return new RelativePath(baseDir, relative);
            } else {
                throw new ForbiddenFilePathException(
                        "Cannot create %s object from file %s - not under baseDir %s"
                                .formatted(
                                        RelativePath.class.getSimpleName(),
                                        file.toString(),
                                        baseDir.toString()));
            }
        } else {
            return new RelativePath(baseDir, file.toString().replace("\\", "/"));
        }
    }

    public static RelativePath of(File baseDir, String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new ConfigurationParseException("Path must not be null or empty");
        }
        return new RelativePath(baseDir, path);
    }

    public File getBaseDir() {
        return baseDir;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public File resolve() {
        if (resolved == null) {
            synchronized (this) {
                if (resolved == null) {
                    resolved = FileHelper.resolveRelativeUnixPath(baseDir, relativePath);
                }
            }
        }
        return resolved;
    }

    public RelativePath rebase(File newBaseDir) {
        if (FileHelper.isUnderDirectory(newBaseDir, this.baseDir)) {
            String relative =
                    newBaseDir
                            .getAbsoluteFile()
                            .toPath()
                            .relativize(resolve().toPath())
                            .toString()
                            .replace("\\", "/")
                            .replaceFirst("^\\.", "");
            return new RelativePath(newBaseDir, relative);
        } else if (FileHelper.isUnderDirectory(this.baseDir, newBaseDir)) {
            String relative =
                    resolve()
                            .toPath()
                            .relativize(newBaseDir.getAbsoluteFile().toPath())
                            .toString()
                            .replace("\\", "/")
                            .replaceFirst("^\\.", "");
            return new RelativePath(newBaseDir, relative);
        }
        throw new IllegalArgumentException(
                "Cannot rebase %s to new base dir %s".formatted(this, newBaseDir));
    }

    public boolean exists() {
        return resolve().exists();
    }

    public String getName() {
        return resolve().getName();
    }

    public boolean isDirectory() {
        return resolve().isDirectory();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelativePath that = (RelativePath) o;
        return baseDir.equals(that.baseDir) && relativePath.equals(that.relativePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseDir, relativePath);
    }

    @Override
    public String toString() {
        return "RelativePath{baseDir=" + baseDir + ", relativePath='" + relativePath + "'}";
    }

    public Resource getRelativePathAsResource() {
        return ResourceFactory.createResource(getRelativePath());
    }

    public RelativePath subDir(String dirName) {
        if (exists() && !isDirectory()) {
            throw new RelativePathException(
                    "Cannot create subpath of %s: not a directory".formatted(this));
        }
        checkDirOrFileName(dirName);
        return new RelativePath(baseDir, getRelativePath() + "/" + dirName);
    }

    private static void checkDirOrFileName(String dirName) {
        if (dirName == null || dirName.isBlank())
            throw new RelativePathException("dirName for new director cannot be null or blank");
        if (dirName.matches(".*[/\\\\:?$%&() ].*")
                || dirName.contains("..")
                || dirName.startsWith(".")) {
            throw new RelativePathException(
                    "dirName for new director cannot contain '/', '\\', ':','?','%','&','(',')',' ', or '..', or start with '.'");
        }
    }

    public RelativePath subFile(String fileName) {
        if (this.exists() && !isDirectory()) {
            throw new RelativePathException(
                    "Cannot create subpath of %s: not a directory".formatted(this));
        }
        checkDirOrFileName(fileName);
        return new RelativePath(baseDir, getRelativePath() + "/" + fileName);
    }
}
