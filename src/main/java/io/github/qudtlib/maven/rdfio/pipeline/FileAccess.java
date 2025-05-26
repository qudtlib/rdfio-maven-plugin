package io.github.qudtlib.maven.rdfio.pipeline;

import io.github.qudtlib.maven.rdfio.common.file.FileAccessException;
import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.file.ForbiddenFilePathException;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.maven.plugin.MojoExecutionException;

public class FileAccess {

    public static void readRdf(RelativePath path, Model model, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        FileHelper.ensureFilesExist(List.of(file), "input");
        state.getLog().debug("Reading RDF from: " + file.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(file)) {
            RDFDataMgr.read(model, fis, RDFDataMgr.determineLang(file.getName(), null, Lang.TTL));
        } catch (IOException e) {
            throw new FileAccessException("Failed to read RDF file: " + file, e);
        }
    }

    public static void readRdf(RelativePath path, Dataset dataset, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        FileHelper.ensureFilesExist(List.of(file), "input");
        state.getLog().debug("Reading RDF from: " + file.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(file)) {
            RDFDataMgr.read(
                    dataset, fis, RDFDataMgr.determineLang(file.getName(), null, Lang.TRIG));
        } catch (IOException e) {
            throw new FileAccessException("Failed to read RDF file: " + file, e);
        }
    }

    public static void readRdf(List<RelativePath> paths, Model model, PipelineState state)
            throws FileAccessException {
        for (RelativePath path : paths) {
            File file = validatePath(path, state);
            try (FileInputStream fis = new FileInputStream(file)) {
                RDFDataMgr.read(
                        model, fis, RDFDataMgr.determineLang(file.getName(), null, Lang.TTL));
            } catch (IOException e) {
                throw new FileAccessException("Failed to load RDF file: " + path, e);
            }
        }
    }

    public static void writeRdf(RelativePath path, Dataset dataset, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        Lang lang = RDFLanguages.resourceNameToLang(file.getName(), Lang.TTL);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        state.getLog().debug("Writing RDF dataset to: " + file.getAbsolutePath());
        try (FileOutputStream fos = new FileOutputStream(file)) {
            RDFDataMgr.write(fos, dataset, lang);
        } catch (IOException e) {
            throw new FileAccessException("Failed to write RDF file: " + file, e);
        }
    }

    public static void writeRdf(RelativePath path, Model model, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        Lang lang = RDFLanguages.resourceNameToLang(file.getName(), Lang.TTL);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        state.getLog().debug("Writing RDF model to: " + file.getAbsolutePath());
        try (FileOutputStream fos = new FileOutputStream(file)) {
            RDFDataMgr.write(fos, model, lang);
        } catch (IOException e) {
            throw new FileAccessException("Failed to write RDF file: " + file, e);
        }
    }

    public static String readText(RelativePath path, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        FileHelper.ensureFilesExist(List.of(file), "input");
        state.getLog().debug("Reading text from: " + file.getAbsolutePath());
        try {
            return Files.readString(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FileAccessException("Failed to read text file: " + file, e);
        }
    }

    public static void writeText(RelativePath path, String content, PipelineState state)
            throws FileAccessException {
        File file = validatePath(path, state);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        state.getLog().debug("Writing text to: " + file.getAbsolutePath());
        try {
            Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new FileAccessException("Failed to write text file: " + file, e);
        }
    }

    public static void delete(RelativePath path, PipelineState state) throws FileAccessException {
        File file = validatePath(path, state);
        state.getLog().debug("Deleting file: " + file.getAbsolutePath());
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new FileAccessException("Failed to delete file: " + file, e);
        }
    }

    public static boolean exists(RelativePath path, PipelineState state) {
        try {
            File file = validatePath(path, state);
            boolean exists = file.exists();
            state.getLog().debug("Checking existence of " + file.getAbsolutePath() + ": " + exists);
            return exists;
        } catch (FileAccessException e) {
            state.getLog().debug("Path validation failed for existence check: " + path, e);
            return false;
        }
    }

    public static boolean createParentFolder(RelativePath path, PipelineState state) {
        File parent = path.resolve().getParentFile();
        if (parent.exists()) {
            return false;
        }
        try {
            state.requireUnderBaseDir(parent);
            return parent.mkdirs();
        } catch (FileAccessException e) {
            throw new ForbiddenFilePathException(e);
        }
    }

    public static boolean mkdirs(RelativePath path, PipelineState pipelineState) {
        File dir = validatePath(path, pipelineState);
        if (path.exists() && !path.isDirectory()) {
            throw new FileAccessException("Cannot mkdirs: %s is not a directory".formatted(path));
        }
        try {
            pipelineState.requireUnderBaseDir(dir);
            return dir.mkdirs();
        } catch (FileAccessException e) {
            throw new ForbiddenFilePathException(e);
        }
    }

    private static File validatePath(RelativePath path, PipelineState state)
            throws FileAccessException {
        File file = path.resolve();
        state.requireUnderBaseDir(file);
        return file;
    }

    public static List<RelativePath> resolveFileSelection(
            FileSelection fileSelection, File baseDir) {
        if (fileSelection == null) {
            return List.of();
        }
        return Arrays.stream(FileHelper.getFilesForFileSelection(fileSelection, baseDir))
                .map(f -> (RelativePath.of(baseDir, f)))
                .toList();
    }

    public static void updateHashWithFiles(
            List<RelativePath> paths, MessageDigest digest, PipelineState state)
            throws MojoExecutionException {
        for (RelativePath path : paths) {
            if (path.exists()) {
                digest.update(path.getRelativePath().getBytes(StandardCharsets.UTF_8));
                digest.update(readText(path, state).getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
