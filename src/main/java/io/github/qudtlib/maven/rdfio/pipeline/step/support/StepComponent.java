package io.github.qudtlib.maven.rdfio.pipeline.step.support;

import io.github.qudtlib.maven.rdfio.pipeline.step.Step;

public interface StepComponent<T extends Step> {

    T getOwner();

    String usage();
}
