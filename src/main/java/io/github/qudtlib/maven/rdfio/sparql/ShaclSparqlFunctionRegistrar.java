package io.github.qudtlib.maven.rdfio.sparql;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.topbraid.shacl.arq.SHACLFunctions;

public class ShaclSparqlFunctionRegistrar {

    public static void registerSHACLFunctions(Model shapesModel) {
        // Register SHACL functions with TopBraid
        SHACLFunctions.registerFunctions(shapesModel);
    }

    public static void executeQuery(Model dataModel, String sparqlQuery) {
        Query query = QueryFactory.create(sparqlQuery);
        try (QueryExecution qExec = QueryExecutionFactory.create(query, dataModel)) {
            ResultSet results = qExec.execSelect();
            while (results.hasNext()) {
                System.out.println(results.next());
            }
        }
    }

    public static void main(String[] args) {
        // Load shapes model
        Model shapesModel = ModelFactory.createDefaultModel();
        shapesModel.read("shaclFunction.ttl");

        // Register SHACL functions
        registerSHACLFunctions(shapesModel);

        // Load data model
        Model dataModel = ModelFactory.createDefaultModel();
        dataModel.read("starwars.ttl");

        // Execute query with custom function
        String queryString =
                "PREFIX ex: <http://example.com/ns#> "
                        + "SELECT ?result WHERE { BIND(ex:myFunction(\"test\") AS ?result) }";
        executeQuery(dataModel, queryString);
    }
}
