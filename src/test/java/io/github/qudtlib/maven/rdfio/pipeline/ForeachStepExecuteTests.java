package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.pipeline.step.AddStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.ForeachStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.SparqlUpdateStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.GraphSelection;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.Values;
import java.io.File;
import java.nio.file.Files;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ForeachStepExecuteTests {
    private Dataset dataset;
    private PipelineState state;
    private File baseDir;
    private String pipelineId;
    private static final String TEST_RDF_FILE = "src/test/resources/data.ttl";
    private static final String EXPECTED_SUBJECT = "http://example.org/s";
    private static final String EXPECTED_PREDICATE = "http://example.org/p";
    private static final String EXPECTED_OBJECT = "http://example.org/o";
    private static final Statement DEFAULT_STATEMENT =
            new StatementImpl(
                    ResourceFactory.createResource(EXPECTED_SUBJECT),
                    ResourceFactory.createProperty(EXPECTED_PREDICATE),
                    ResourceFactory.createResource(EXPECTED_OBJECT));
    private static final Statement EXPECTED_STATEMENT =
            new StatementImpl(
                    ResourceFactory.createResource("http://example.org/s2"),
                    ResourceFactory.createProperty("http://example.org/p2"),
                    ResourceFactory.createResource("http://example.org/o2"));

    @BeforeEach
    void setUp() throws Exception {
        dataset = DatasetFactory.create();
        baseDir = new File(".");
        RelativePath workBaseDir = new RelativePath(baseDir, "target");
        baseDir.mkdirs();
        pipelineId = "test-pipeline";
        state =
                new PipelineState(
                        pipelineId,
                        baseDir,
                        workBaseDir.subDir("rdfio").subDir("pipelines"),
                        new StdoutLog(),
                        null,
                        null);
        state.files().mkdirs(workBaseDir);
        // Ensure test RDF file exists
        File rdfFile = new File(baseDir, TEST_RDF_FILE);
        if (!rdfFile.exists()) {
            Files.createDirectories(rdfFile.getParentFile().toPath());
            String content =
                    "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";
            Files.write(rdfFile.toPath(), content.getBytes());
        }
    }

    @Test
    void testExecuteWithValidGraphSelectionAndSparqlUpdate() throws Exception {
        // Arrange: Populate dataset with two graphs
        Model model1 = dataset.getNamedModel("vocab:test1");
        model1.add(
                ResourceFactory.createResource("http://example.org/dummy1"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        Model model2 = dataset.getNamedModel("vocab:test2");
        model2.add(
                ResourceFactory.createResource("http://example.org/dummy2"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");
        step.addBodyStep(bodyStep);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify both graphs have the inserted triple
        assertTrue(
                model1.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Graph vocab:test1 should contain the inserted triple");
        assertTrue(
                model2.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Graph vocab:test2 should contain the inserted triple");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithMultipleBodySteps() throws Exception {
        // Arrange: Populate dataset with one graph
        Model sourceModel = dataset.getNamedModel("test:graph");
        sourceModel.add(
                ResourceFactory.createResource("http://example.org/dummy"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        ForeachStep step = new ForeachStep();
        step.setVar("graph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("test:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);

        // Body step 1: AddStep to copy to a new graph
        AddStep addStep = new AddStep();
        addStep.getInputsComponent().addGraph("${graph}");
        addStep.setToGraph("processed:graph");
        step.addBodyStep(addStep);

        // Body step 2: SparqlUpdateStep to add a triple
        SparqlUpdateStep sparqlStep = new SparqlUpdateStep();
        sparqlStep.setSparql(
                "INSERT { GRAPH ?graph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");
        step.addBodyStep(sparqlStep);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify results
        Model processedModel = dataset.getNamedModel("processed:graph");
        assertTrue(
                processedModel.contains(
                        ResourceFactory.createResource("http://example.org/dummy"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Processed graph should contain copied triple");
        assertTrue(
                sourceModel.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Source graph should contain inserted triple");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithEmptyGraphSelection() throws Exception {
        // Arrange: No matching graphs
        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("nonexistent:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");
        step.addBodyStep(bodyStep);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Dataset should remain unchanged
        assertTrue(dataset.isEmpty(), "Dataset should remain empty with no matching graphs");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteMissingVar() throws Exception {
        // Arrange: ForeachStep with missing var
        ForeachStep step = new ForeachStep();
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");
        step.addBodyStep(bodyStep);

        // Act & Assert: Expect exception
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when var is missing");
    }

    @Test
    void testExecuteMissingValues() throws Exception {
        // Arrange: ForeachStep with missing values
        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");
        step.addBodyStep(bodyStep);

        // Act & Assert: Expect exception
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when values are missing");
    }

    @Test
    void testExecuteMissingBody() throws Exception {
        // Arrange: ForeachStep with missing body
        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);

        // Act & Assert: Expect exception
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when body is missing");
    }

    @Test
    void testExecuteWithFileInputAndNestedAddStep() throws Exception {
        // Arrange: Populate dataset via file
        AddStep initialAdd = new AddStep();
        initialAdd.getInputsComponent().addFile(TEST_RDF_FILE);
        initialAdd.setToGraph("test:graph");
        initialAdd.execute(dataset, state);

        ForeachStep step = new ForeachStep();
        step.setVar("graph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("test:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        AddStep bodyAdd = new AddStep();
        bodyAdd.getInputsComponent().addGraph("${graph}");
        bodyAdd.setToGraph("processed:graph");
        step.addBodyStep(bodyAdd);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify the triple was copied to the new graph
        Model processedModel = dataset.getNamedModel("processed:graph");
        assertTrue(
                processedModel.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Processed graph should contain the triple from the source graph");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithInvalidBodyStep() throws Exception {
        // Arrange: Populate dataset with one graph
        dataset.getNamedModel("test:graph").add(DEFAULT_STATEMENT);

        ForeachStep step = new ForeachStep();
        step.setVar("graph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("test:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql("INVALID SPARQL"); // Malformed SPARQL
        step.addBodyStep(bodyStep);

        // Act & Assert: Expect exception
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when body step fails (malformed SPARQL)");
    }

    @Test
    void testExecutePreservesExistingMetadata() throws Exception {
        // Arrange: Populate metadata graph
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("otherVar"),
                RDFIO.value,
                ResourceFactory.createResource("other:value"));

        // Populate dataset with one graph
        dataset.getNamedModel("test:graph").add(DEFAULT_STATEMENT);

        ForeachStep step = new ForeachStep();
        step.setVar("graph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("test:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?graph { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> } } WHERE {}");
        step.addBodyStep(bodyStep);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify metadata preserved
        assertTrue(
                metaModel.contains(
                        RDFIO.makeVariableUri("otherVar"),
                        RDFIO.value,
                        ResourceFactory.createResource("other:value")),
                "Existing metadata should be preserved");
        assertTrue(
                dataset.getNamedModel("test:graph").contains(EXPECTED_STATEMENT),
                "Graph should contain inserted triple");
    }

    @Test
    void testExecuteNestedForeach() throws Exception {
        // Arrange: Populate dataset with outer and inner graphs
        dataset.getNamedModel("outer:graph1").add(DEFAULT_STATEMENT);
        dataset.getNamedModel("inner:graph1").add(DEFAULT_STATEMENT);
        dataset.getNamedModel("inner:graph2").add(DEFAULT_STATEMENT);

        ForeachStep outerStep = new ForeachStep();
        outerStep.setVar("outerGraph");
        GraphSelection outerGraphs = new GraphSelection();
        outerGraphs.addInclude("outer:*");
        Values outerValues = new Values();
        outerValues.setGraphs(outerGraphs);
        outerStep.setValues(outerValues);

        ForeachStep innerStep = new ForeachStep();
        innerStep.setVar("innerGraph");
        GraphSelection innerGraphs = new GraphSelection();
        innerGraphs.addInclude("inner:*");
        Values innerValues = new Values();
        innerValues.setGraphs(innerGraphs);
        innerStep.setValues(innerValues);

        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT { GRAPH ?innerGraph { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> } } WHERE {}");
        innerStep.addBodyStep(bodyStep);

        outerStep.addBodyStep(innerStep);

        // Act: Execute the step
        outerStep.execute(dataset, state);
        // Assert: Verify inner graphs have the inserted triple
        assertTrue(
                dataset.getNamedModel("inner:graph1")
                        .contains(
                                ResourceFactory.createResource("http://example.org/s2"),
                                ResourceFactory.createProperty("http://example.org/p2"),
                                ResourceFactory.createResource("http://example.org/o2")),
                "Graph inner:graph1 should contain inserted triple");
        assertTrue(
                dataset.getNamedModel("inner:graph2")
                        .contains(
                                ResourceFactory.createResource("http://example.org/s2"),
                                ResourceFactory.createProperty("http://example.org/p2"),
                                ResourceFactory.createResource("http://example.org/o2")),
                "Graph inner:graph2 should contain inserted triple");
        assertTrue(
                state.getPrecedingSteps().contains(outerStep),
                "Outer step should be added to preceding steps");
    }
}
