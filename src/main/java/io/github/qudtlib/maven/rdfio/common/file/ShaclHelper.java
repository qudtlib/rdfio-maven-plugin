package io.github.qudtlib.maven.rdfio.common.file;

import static io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.addPrefixes;
import static io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.withLineNumbers;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.binding.Binding;

public class ShaclHelper {
    public static String formatValidationReport(Model validationReport) {
        Dataset ds = DatasetFactory.create(validationReport);
        String sparql =
                """
                SELECT * WHERE
                {
                  VALUES ( ?resultSeverity ?resultSeverityIndex ) {
                      ( sh:Violation 3 )
                      ( sh:Warning   2 )
                      ( sh:Info      1 )
                  }
                  VALUES ( ?resultSeverity ?resultSeverityLabel ) {
                      ( sh:Violation "Violation" )
                      ( sh:Warning   "Warning" )
                      ( sh:Info      "Info" )
                  }
                  ?report
                    a            sh:ValidationReport;
                    sh:result    ?result .
                  ?result
                    a                   sh:ValidationResult;
                    sh:focusNode        ?focusNode;
                    sh:resultMessage    ?resultMessage;
                    sh:resultSeverity   ?resultSeverity.
               } ORDER BY DESC(?resultSeverityIndex) ?focusNode ?resultMessage
               """;
        StringBuilder sb = new StringBuilder();
        sb.append(
                """
                SHACL VALIDATION REPORT
                =======================

                """);
        try {
            sparql = addPrefixes(sparql, ds);
            Query query = QueryFactory.create(sparql);
            try (QueryExecution qe = QueryExecutionFactory.create(query, ds)) {
                ResultSet result = null;
                try {
                    result = qe.execSelect();
                    while (result.hasNext()) {
                        Binding binding = result.nextBinding();
                        String focusNode = binding.get("focusNode").getURI();
                        String severity =
                                binding.get("resultSeverityLabel").getLiteral().toString();
                        String resultMessage = binding.get("resultMessage").getLiteral().toString();
                        String resultString =
                                String.format(
                                        """
                                Severity  : %s
                                Focus node: %s
                                Message   : %s

                                """
                                                .formatted(focusNode, severity, resultMessage));
                        sb.append(resultString);
                    }
                } catch (Exception e) {
                    if (result != null) {
                        result.close();
                    }
                    sb.append(e.getMessage());
                }
            }
        } catch (Exception e) {
            sb.append("Failed to execute SPARQL query:\n" + withLineNumbers(sparql));
        }
        return sb.toString();
    }
}
