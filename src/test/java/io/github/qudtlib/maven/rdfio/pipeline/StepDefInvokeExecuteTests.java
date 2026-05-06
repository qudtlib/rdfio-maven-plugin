package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.pipeline.step.InvokeStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.SparqlUpdateStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.StepDefStep;
import java.io.File;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StepDefInvokeExecuteTests {

    private Dataset dataset;
    private PipelineState state;

    @BeforeEach
    void setUp() throws Exception {
        dataset = DatasetFactory.create();
        File baseDir = new File(".");
        RelativePath workBaseDir = new RelativePath(baseDir, "target");
        state =
                new PipelineState(
                        "test-pipeline",
                        baseDir,
                        workBaseDir.subDir("rdfio").subDir("pipelines"),
                        new StdoutLog(),
                        null,
                        null);
        state.files().mkdirs(workBaseDir);
    }

    @Test
    void testStepDefRegistersAndInvokeExecutes() throws Exception {
        // Build a stepDef with one sparqlUpdate
        SparqlUpdateStep updateStep = new SparqlUpdateStep();
        updateStep.setSparql("INSERT DATA { GRAPH <test:graph> { <urn:s> <urn:p> <urn:o> } }");

        StepDefStep stepDef = new StepDefStep();
        stepDef.setId("my-def");
        stepDef.addStep(updateStep);

        // Execute stepDef — should register, not execute the inner step
        stepDef.execute(dataset, state);
        assertFalse(
                dataset.containsNamedModel("test:graph")
                        && dataset.getNamedModel("test:graph")
                                .contains(
                                        ResourceFactory.createResource("urn:s"),
                                        ResourceFactory.createProperty("urn:p"),
                                        ResourceFactory.createResource("urn:o")),
                "StepDef.execute should not run the inner steps");

        // Execute invoke — now the inner step should run
        InvokeStep invoke = new InvokeStep();
        invoke.setStepRef("my-def");
        invoke.execute(dataset, state);

        assertTrue(
                dataset.getNamedModel("test:graph")
                        .contains(
                                ResourceFactory.createResource("urn:s"),
                                ResourceFactory.createProperty("urn:p"),
                                ResourceFactory.createResource("urn:o")),
                "Invoke should have executed the registered step");
    }

    @Test
    void testInvokeCanBeCalledMultipleTimes() throws Exception {
        SparqlUpdateStep updateStep = new SparqlUpdateStep();
        updateStep.setSparql("INSERT DATA { GRAPH <counter:graph> { <urn:s> <urn:p> <urn:o> } }");

        StepDefStep stepDef = new StepDefStep();
        stepDef.setId("repeatable");
        stepDef.addStep(updateStep);
        stepDef.execute(dataset, state);

        InvokeStep invoke = new InvokeStep();
        invoke.setStepRef("repeatable");

        // Invoke twice — idempotent insert, graph still exists after both
        invoke.execute(dataset, state);
        invoke.execute(dataset, state);

        assertTrue(
                dataset.containsNamedModel("counter:graph"),
                "Graph should exist after two invocations");
    }

    @Test
    void testInvokeUnknownStepRefThrows() throws Exception {
        InvokeStep invoke = new InvokeStep();
        invoke.setStepRef("does-not-exist");
        assertThrows(
                MojoExecutionException.class,
                () -> invoke.execute(dataset, state),
                "Invoke with unknown stepRef should throw");
    }

    @Test
    void testStepDefWithMultipleStepsExecutesAllOnInvoke() throws Exception {
        SparqlUpdateStep step1 = new SparqlUpdateStep();
        step1.setSparql("INSERT DATA { GRAPH <g1:graph> { <urn:s1> <urn:p> <urn:o> } }");

        SparqlUpdateStep step2 = new SparqlUpdateStep();
        step2.setSparql("INSERT DATA { GRAPH <g2:graph> { <urn:s2> <urn:p> <urn:o> } }");

        StepDefStep stepDef = new StepDefStep();
        stepDef.setId("multi");
        stepDef.addStep(step1);
        stepDef.addStep(step2);
        stepDef.execute(dataset, state);

        InvokeStep invoke = new InvokeStep();
        invoke.setStepRef("multi");
        invoke.execute(dataset, state);

        assertTrue(dataset.containsNamedModel("g1:graph"), "g1:graph should be created");
        assertTrue(dataset.containsNamedModel("g2:graph"), "g2:graph should be created");
    }
}
