package io.github.qudtlib.maven.rdfio.pipeline.support;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.RDFNode;

public class VariableResolver {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    public static String resolveVariables(String input, Dataset dataset, String metadataGraph) {
        if (input == null || !input.contains("${")) {
            return input;
        }
        QuerySolutionMap bindings = SparqlHelper.extractVariableBindings(dataset, metadataGraph);
        return replaceVariables(input, bindings);
    }

    public static List<String> resolveVariables(
            List<String> input, Dataset dataset, String metadataGraph) {
        if (input == null || input.isEmpty() || input.stream().noneMatch(s -> s.contains("${"))) {
            return input;
        }
        QuerySolutionMap bindings = SparqlHelper.extractVariableBindings(dataset, metadataGraph);
        return input.stream().map(s -> replaceVariables(s, bindings)).toList();
    }

    public static FileSelection resolveVariables(
            FileSelection input, Dataset dataset, String metadataGraph) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return new FileSelection(
                resolveVariables(input.getInclude(), dataset, metadataGraph),
                resolveVariables(input.getExclude(), dataset, metadataGraph));
    }

    public static GraphSelection resolveVariables(
            GraphSelection input, Dataset dataset, String metadataGraph) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return new GraphSelection(
                resolveVariables(input.getInclude(), dataset, metadataGraph),
                resolveVariables(input.getExclude(), dataset, metadataGraph));
    }

    private static String replaceVariables(String input, QuerySolutionMap bindings) {
        String result = input;
        Matcher matcher = VARIABLE_PATTERN.matcher(input);
        while (matcher.find()) {
            String varName = matcher.group(1);
            RDFNode value = bindings.get(varName);
            if (value == null) {
                throw new PipelineConfigurationExeception(
                        "No binding found for variable '" + varName + "' in metadata graph");
            }
            String replacement =
                    value.isLiteral() ? value.asLiteral().getString() : value.asResource().getURI();
            result = result.replace("${" + varName + "}", replacement);
        }
        return result;
    }
}
