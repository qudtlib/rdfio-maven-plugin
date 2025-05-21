package io.github.qudtlib.maven.rdfio.pipeline;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

public class PipelineHelper {

    public static List<String> getGraphs(Dataset dataset, GraphSelection graphSelection) {
        if (graphSelection == null) return new ArrayList<>();
        List<String> result = new ArrayList<>();
        List<Pattern> includePatterns = new ArrayList<>();
        List<Pattern> excludePatterns = new ArrayList<>();

        // Convert include patterns to regex
        for (String include : graphSelection.getIncludes()) {
            includePatterns.add(Pattern.compile(wildcardToRegex(include)));
        }

        // Convert exclude patterns to regex
        for (String exclude : graphSelection.getExcludes()) {
            excludePatterns.add(Pattern.compile(wildcardToRegex(exclude)));
        }

        // Match graph names against patterns
        dataset.listModelNames()
                .forEachRemaining(
                        name -> {
                            for (Pattern includePattern : includePatterns) {
                                if (includePattern.matcher(name.toString()).matches()) {
                                    boolean excluded = false;
                                    for (Pattern excludePattern : excludePatterns) {
                                        if (excludePattern.matcher(name.toString()).matches()) {
                                            excluded = true;
                                            break;
                                        }
                                    }
                                    if (!excluded) {
                                        result.add(name.toString());
                                    }
                                    break; // Stop checking other include patterns once matched
                                }
                            }
                        });

        return result;
    }

    private static String wildcardToRegex(String wildcard) {
        StringBuilder regex = new StringBuilder();
        regex.append("^"); // Start of string
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    regex.append(".*"); // Match any characters
                    break;
                case '?':
                    regex.append("."); // Match any single character
                    break;
                default:
                    regex.append(Pattern.quote(String.valueOf(c))); // Escape special characters
                    break;
            }
        }
        regex.append("$"); // End of string
        return regex.toString();
    }

    /**
     * Prints the dataset to a String in pretty-printed TRIG format.
     *
     * @param dataset The Jena Dataset to serialize.
     * @return A String containing the dataset in TRIG format.
     * @throws RuntimeException if serialization fails.
     */
    public static String datasetToPrettyTrig(Dataset dataset) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            RDFDataMgr.write(out, dataset, RDFFormat.TRIG_PRETTY);
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize dataset to TRIG", e);
        }
    }

    public static void clearDataset(Dataset dataset) {
        dataset.getDefaultModel().removeAll();
        dataset.listModelNames()
                .forEachRemaining(graph -> dataset.getNamedModel(graph).removeAll());
        for (Iterator<String> it = dataset.listNames(); it.hasNext(); it.next()) {
            it.remove();
        }
    }

    public static void ensureGraphsExist(Dataset dataset, List<String> graphs, String kind) {
        for (String graph : graphs) {
            if (!dataset.containsNamedModel(graph)) {
                throw new PipelineConfigurationExeception(
                        "Configured %s graph %s does not exist in dataset".formatted(kind, graph));
            }
        }
    }

    static String serializeMessageDigest(MessageDigest digest) {
        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
