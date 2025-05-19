package io.github.qudtlib.maven.rdfio.filter;

import static io.github.qudtlib.maven.rdfio.filter.GraphsHelper.getAllModels;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;

public class SparqlHelper {
    public static String addPrefixes(String update, Model model) {
        String prefixes =
                model.getNsPrefixMap().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> String.format("PREFIX %s: <%s>", e.getKey(), e.getValue()))
                        .collect(Collectors.joining("\n"));
        return prefixes + "\n" + update;
    }

    public static String addPrefixes(String sparql, Dataset dataset) {
        String prefixes =
                getAllModels(dataset).stream()
                        .map(Model::getNsPrefixMap)
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .distinct()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> String.format("PREFIX %s: <%s>", e.getKey(), e.getValue()))
                        .collect(Collectors.joining("\n"));
        return prefixes + "\n" + sparql;
    }

    public static String withLineNumbers(String updateWithPrefixes) {
        String[] lines = updateWithPrefixes.split("\n");
        int width = String.valueOf(lines.length).length();
        return IntStream.range(0, lines.length)
                .mapToObj(i -> String.format("%" + width + "d %s", i + 1, lines[i]))
                .collect(Collectors.joining("\n"));
    }
}
