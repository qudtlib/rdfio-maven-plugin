package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public interface StepComponent<T extends Step> {

    T getOwner();

    static <T extends Step> StepComponent<T> parse(T owner, Xpp3Dom config) {
        throw new UnsupportedOperationException("implementor specific!");
    }

    String usage();
}
