package io.github.qudtlib.maven.rdfio.common.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;

public class RdfFileProcessor {
    public static List<File> resolveFiles(
            List<String> files, FileSelection fileSelection, File baseDir) {
        List<File> resolved = new ArrayList<>();
        if (files != null) {
            files.forEach(f -> resolved.add(resolveFile(baseDir, f)));
        }
        if (fileSelection != null) {
            Arrays.stream(FileHelper.getFilesForFileSelection(fileSelection, baseDir))
                    .map(f -> resolveFile(baseDir, f))
                    .forEach(resolved::add);
        }
        return resolved;
    }

    public static File resolveFile(File baseDir, String filePath) {
        return baseDir.toPath().resolve(filePath).toFile();
    }

    public static void loadRdfFiles(List<File> files, Model model) throws MojoExecutionException {
        for (File file : files) {
            try (FileInputStream fis = new FileInputStream(file)) {
                RDFDataMgr.read(
                        model, fis, RDFDataMgr.determineLang(file.getName(), null, Lang.TTL));
            } catch (IOException e) {
                throw new MojoExecutionException("Failed to load RDF file: " + file, e);
            }
        }
    }

    public static void updateHashWithFiles(List<File> files, MessageDigest digest)
            throws IOException {
        for (File file : files) {
            if (file.exists()) {
                digest.update(file.getPath().getBytes(StandardCharsets.UTF_8));
                digest.update(Files.readAllBytes(file.toPath()));
            }
        }
    }
}
