package io.github.qudtlib.maven.rdfio.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class AbstractPredicateFilter extends AbstractFilter implements Filter {

    protected boolean include;

    public AbstractPredicateFilter(boolean include) {
        this.include = include;
    }

    protected List<Predicate<Statement>> statementPredicates = new ArrayList<>();

    public void filter(Model model) {
        StmtIterator it = model.listStatements();
        while (it.hasNext()) {
            Statement stmt = it.nextStatement();
            if (include) {
                if (statementPredicates.stream().noneMatch(pred -> pred.test(stmt))) {
                    it.remove();
                }
            } else {
                if (statementPredicates.stream().anyMatch(pred -> pred.test(stmt))) {
                    it.remove();
                }
            }
        }
    }

    @Parameter
    public void setPredicate(String predicate) {
        this.statementPredicates.add(new PredicateTest(predicate));
    }
}
