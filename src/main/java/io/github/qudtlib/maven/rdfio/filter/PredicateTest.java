package io.github.qudtlib.maven.rdfio.filter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;

public class PredicateTest implements Predicate<Statement> {
    private final String predicate;
    private final AtomicReference<Resource> predicateResourceRef = new AtomicReference<>();

    public PredicateTest(String predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean test(Statement statement) {
        Resource predicateResource = lazyGetPredicateResource(statement);
        boolean match = statement.getPredicate().equals(predicateResource);
        return match;
    }

    private Resource lazyGetPredicateResource(Statement statement) {
        return predicateResourceRef.updateAndGet(
                current -> {
                    if (current != null) {
                        return current;
                    }
                    Map<String, String> prefixMap = statement.getModel().getNsPrefixMap();
                    String predicateIri = expandIfPrefixed(predicate, prefixMap);
                    return ResourceFactory.createResource(predicateIri);
                });
    }

    private String expandIfPrefixed(String predicate, Map<String, String> prefixMap) {
        String prefix = predicate.replaceAll(":.+$", "");
        if (prefixMap.containsKey(prefix)) {
            return prefixMap.get(prefix) + predicate.substring(prefix.length() + 1);
        }
        return predicate;
    }
}
