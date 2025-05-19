package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.update.UpdateAction;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Helper class for executing SPARQL queries and updates with variable bindings from the metadata
 * graph.
 */
public class SparqlHelper {

    /** Interface for processing SPARQL query results. */
    public interface QueryResultProcessor {
        default void processAskResult(boolean result) {}

        default void processSelectResult(ResultSet result) {}

        default void processConstructOrDescribeResult(Model result) {}
    }

    /**
     * Extracts variable bindings from the metadata graph into a QuerySolutionMap. Variables are
     * extracted from triples like <http://qudtlib.org/rdfio/variables/varName> rdfio:value "value".
     *
     * @param dataset The Jena Dataset containing the metadata graph.
     * @param metadataGraph The URI of the metadata graph (e.g., http://qudtlib.org/rdfio/metadata).
     * @return A QuerySolutionMap with variable bindings.
     */
    private static QuerySolutionMap extractVariableBindings(Dataset dataset, String metadataGraph) {
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
        QuerySolutionMap bindings = extractVariableBindings(dataset, metadataGraph);
        try {
            UpdateAction.parseExecute(sparql, dataset, bindings);
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to execute SPARQL update: " + sparql, e);
        }
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
            throw new MojoExecutionException("Failed to execute SPARQL query: " + sparql, e);
        }
    }
}
