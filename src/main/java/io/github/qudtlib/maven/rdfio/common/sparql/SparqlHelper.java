package io.github.qudtlib.maven.rdfio.common.sparql;

import static io.github.qudtlib.maven.rdfio.filter.GraphsHelper.getAllModels;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.sparql.ShaclSparqlFunctionRegistrar;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.stream.Streams;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.topbraid.shacl.vocabulary.SH;

public class SparqlHelper {
    public static String addPrefixes(String update, Model model) {
        String prefixes =
                model.getNsPrefixMap().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> String.format("PREFIX %s: <%s>", e.getKey(), e.getValue()))
                        .collect(Collectors.joining("\n"));
        return prefixes.trim().isBlank() ? update : prefixes + "\n" + update;
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
        return prefixes.trim().isBlank() ? sparql : prefixes + "\n" + sparql;
    }

    public static String withLineNumbers(String updateWithPrefixes) {
        String[] lines = updateWithPrefixes.split("\n");
        int width = String.valueOf(lines.length).length();
        return IntStream.range(0, lines.length)
                .mapToObj(i -> String.format("%" + width + "d %s", i + 1, lines[i]))
                .collect(Collectors.joining("\n"));
    }

    /**
     * Extracts variable bindings from the metadata graph into a QuerySolutionMap. Variables are
     * extracted from triples like <http://qudtlib.org/rdfio/variables/varName> rdfio:value "value".
     *
     * @param dataset The Jena Dataset containing the metadata graph.
     * @param metadataGraph The URI of the metadata graph (e.g., http://qudtlib.org/rdfio/metadata).
     * @return A QuerySolutionMap with variable bindings.
     */
    public static QuerySolutionMap extractVariableBindings(Dataset dataset, String metadataGraph) {
        QuerySolutionMap bindings = new QuerySolutionMap();
        Model metaModel = dataset.getNamedModel(metadataGraph);
        for (Statement stmt :
                metaModel.listStatements(null, RDFIO.value, (RDFNode) null).toList()) {
            if (stmt.getSubject().isURIResource()
                    && stmt.getSubject().getURI().startsWith(RDFIO.VARIABLE_PREFIX)) {
                String varName =
                        stmt.getSubject().getURI().substring(RDFIO.VARIABLE_PREFIX.length());
                RDFNode valueNode = stmt.getObject();
                if (valueNode.isLiteral()) {
                    bindings.add(varName, valueNode.asLiteral());
                } else if (valueNode.isResource()) {
                    bindings.add(varName, valueNode.asResource());
                }
            }
        }
        return bindings;
    }

    /**
     * Executes a SPARQL update query on the dataset, pre-binding variables from the metadata graph.
     *
     * @param sparql The SPARQL update query to execute.
     * @param dataset The Jena Dataset to update.
     * @param metadataGraph The URI of the metadata graph (e.g., http://qudtlib.org/rdfio/metadata).
     * @throws MojoExecutionException If the query is invalid or execution fails.
     */
    public static void executeSparqlUpdateWithVariables(
            String sparql, Dataset dataset, String metadataGraph) throws MojoExecutionException {
        sparql = addPrefixes(sparql, dataset);
        QuerySolutionMap bindings = extractVariableBindings(dataset, metadataGraph);
        UpdateRequest parsedUpdate;
        try {
            parsedUpdate = UpdateFactory.create(sparql);
        } catch (Exception e) {
            throw new MojoExecutionException(
                    "Failed to execute SPARQL update:\n"
                            + withLineNumbers(sparql)
                            + "\n"
                            + e.getMessage());
        }
        UpdateAction.execute(parsedUpdate, dataset, bindings);
    }

    /**
     * Executes a SPARQL update query on the dataset, pre-binding variables from the metadata graph.
     *
     * @param sparql The SPARQL update query to execute.
     * @param dataset The Jena Dataset to update.
     * @param metadataGraph The URI of the metadata graph (e.g., http://qudtlib.org/rdfio/metadata).
     * @throws MojoExecutionException If the query is invalid or execution fails.
     */
    public static void executeSparqlAskWithVariables(
            String sparql, Dataset dataset, String metadataGraph) throws MojoExecutionException {
        sparql = addPrefixes(sparql, dataset);
        QuerySolutionMap bindings = extractVariableBindings(dataset, metadataGraph);
        UpdateRequest parsedUpdate;
        try {
            parsedUpdate = UpdateFactory.create(sparql);
        } catch (Exception e) {
            throw new MojoExecutionException(
                    "Failed to execute SPARQL update:\n"
                            + withLineNumbers(sparql)
                            + "\n"
                            + e.getMessage());
        }
        UpdateAction.execute(parsedUpdate, dataset, bindings);
    }

    /**
     * Executes a SPARQL query (SELECT, ASK, CONSTRUCT, or DESCRIBE) on the dataset, pre-binding
     * variables from the metadata graph, and processes the result using the provided
     * QueryResultProcessor.
     *
     * @param sparql The SPARQL query to execute.
     * @param dataset The Jena Dataset to query.
     * @param metadataGraph The URI of the metadata graph (e.g., http://qudtlib.org/rdfio/metadata).
     * @param processor The QueryResultProcessor to handle the query result.
     * @throws MojoExecutionException If the query is invalid or execution fails.
     */
    public static void executeSparqlQueryWithVariables(
            String sparql, Dataset dataset, String metadataGraph, QueryResultProcessor processor)
            throws MojoExecutionException {
        try {
            sparql = addPrefixes(sparql, dataset);
            Query query = QueryFactory.create(sparql);
            QuerySolutionMap bindings = extractVariableBindings(dataset, metadataGraph);
            try (QueryExecution qe = QueryExecutionFactory.create(query, dataset, bindings)) {
                if (query.isSelectType()) {
                    processor.processSelectResult(qe.execSelect());
                } else if (query.isAskType()) {
                    processor.processAskResult(qe.execAsk());
                } else if (query.isConstructType() || query.isDescribeType()) {
                    Model result = query.isConstructType() ? qe.execConstruct() : qe.execDescribe();
                    processor.processConstructOrDescribeResult(result);
                } else {
                    throw new MojoExecutionException("Unsupported query type: " + sparql);
                }
            }
        } catch (Exception e) {
            throw new MojoExecutionException(
                    "Failed to execute SPARQL query:\n" + withLineNumbers(sparql), e);
        }
    }

    public static void registerShaclFunctions(Dataset dataset, String graphName, Log log) {
        log.debug("Registering SHACL functions");
        // we have to enable simple subclass reasoning such that a sh:SPARQLFunction is recognized
        // as a sh:Function
        Model shaclFunctionsModel = dataset.getNamedModel(graphName);
        // hack: we need to make sh:SPARQLFunction a subclass of sh:Function, otherwise topbraid
        // will not find it.
        shaclFunctionsModel.add(SH.SPARQLFunction, RDFS.subClassOf, SH.Function);
        log.debug("sh:Function nodes found in loaded RDF:");
        shaclFunctionsModel
                .listSubjectsWithProperty(RDF.type, SH.Function)
                .forEachRemaining(res -> log.debug("    " + res.toString()));
        log.debug("sh:SPARQLFunction nodes found in loaded RDF:");
        shaclFunctionsModel
                .listSubjectsWithProperty(RDF.type, SH.SPARQLFunction)
                .forEachRemaining(res -> log.debug("    " + res.toString()));
        ShaclSparqlFunctionRegistrar.registerSHACLFunctions(shaclFunctionsModel);
        FunctionRegistry registry = FunctionRegistry.get();
        log.debug("Registered functions: ");
        Streams.of(registry.keys()).sorted().forEach(uri -> log.debug("    " + uri));
    }

    /** Interface for processing SPARQL query results. */
    public interface QueryResultProcessor {
        default void processAskResult(boolean result) {
            throw new UnsupportedOperationException("Unexpected SPARQL ASK query");
        }

        default void processSelectResult(ResultSet result) {
            throw new UnsupportedOperationException("Unexpected SPARQL SELECT query");
        }

        default void processConstructOrDescribeResult(Model result) {
            throw new UnsupportedOperationException(
                    "Unexpected SPARQL CONSTRUCT or DESCRIBE query");
        }
    }
}
