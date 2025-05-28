package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.*;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.*;
import io.github.qudtlib.maven.rdfio.pipeline.support.PipelineConfigurationExeception;
import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipelineStepsTests {
    private Dataset dataset;
    private PipelineState state;
    private RelativePath testOutputBase;
    private File baseDir;
    private String pipelineId;

    @BeforeEach
    void setUp() {
        dataset = DatasetFactory.create();
        pipelineId = "test-pipeline";
        baseDir = new File(".");
        baseDir.mkdirs();
        RelativePath workBaseDir = new RelativePath(baseDir, "target");
        testOutputBase = workBaseDir.subDir("test-output");
        state =
                new PipelineState(
                        pipelineId,
                        baseDir,
                        workBaseDir.subDir("rdfio").subDir("pipelines"),
                        new StdoutLog(),
                        null,
                        null);
        state.files().mkdirs(workBaseDir);
        state.files().mkdirs(testOutputBase);
        createShapesAndDataFiles(state);
    }

    private static void createShapesAndDataFiles(PipelineState state) {
        String shapesTtl =
                """
                        @prefix sh: <http://www.w3.org/ns/shacl#> .
                        @prefix ex: <http://example.org/> .
                        ex:InferRule a sh:NodeShape ; sh:targetNode ex:s ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:inferred ; sh:object ex:NewObject ] .""";
        String dataTtl = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";

        RelativePath shapesFile = state.files().makeRelativeToOutputBase("test-data/shapes.ttl");
        RelativePath dataFile = state.files().makeRelativeToOutputBase("test-data/data.ttl");
        state.files().createParentFolder(shapesFile);
        state.files().createParentFolder(dataFile);
        state.files().writeText(shapesFile, shapesTtl);
        state.files().writeText(dataFile, dataTtl);
    }

    @Test
    void testAddStepWithFile() throws MojoExecutionException, IOException {
        AddStep step = new AddStep();
        String fileArg = "target/test-output/input.ttl";
        step.getInputsComponent().addFile(fileArg);
        step.setToGraph("test:graph");
        String ttl = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";
        File inputFile = FileHelper.resolveRelativeUnixPath(baseDir, fileArg);
        inputFile.getParentFile().mkdirs();
        Files.write(inputFile.toPath(), ttl.getBytes());

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
        String graphName = "test:graph";
        System.out.println(PipelineHelper.formatPipelineStateSummary(dataset, state));
        boolean found = PipelineHelper.isFileBoundToGraph(dataset, state, fileArg, graphName);
        assertTrue(found);
    }

    @Test
    void testAddStepWithNonExistentFile() {
        AddStep step = new AddStep();
        step.getInputsComponent().addFile("target/rdfio/test-data/nonexistent.ttl");
        step.setToGraph("test:graph");

        assertThrows(
                PipelineConfigurationExeception.class,
                () -> step.execute(dataset, state),
                "Should throw when loading non-existent file");
    }

    @Test
    void testAddStepWithMissingToGraph() throws MojoExecutionException {
        AddStep step = new AddStep();
        step.getInputsComponent().addFile("src/test/resources/data.ttl");
        step.execute(dataset, state);
        assertTrue(
                dataset.getDefaultModel()
                        .contains(
                                ResourceFactory.createResource("http://example.org/s"),
                                ResourceFactory.createProperty("http://example.org/p"),
                                ResourceFactory.createResource("http://example.org/o")),
                "Default graph should contain the triple from the file");
    }

    @Test
    void testAddStepWithEmptyFiles() throws MojoExecutionException {
        AddStep step = new AddStep();
        FileSelection files = new FileSelection();
        files.addInclude("src/test/resources/dontfind/*.ttl");
        step.getInputsComponent().setFileSelection(files);
        step.setToGraph("test:graph");

        step.execute(dataset, state);
        Model model = dataset.getNamedModel("test:graph");
        assertTrue(model.isEmpty(), "Model should be empty with no matching files");
    }

    @Test
    void testAddStepWithGraphCopy() throws MojoExecutionException {
        Model sourceModel = dataset.getNamedModel("source:graph");
        sourceModel.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        AddStep step = new AddStep();
        step.getInputsComponent().addGraph("source:graph");
        step.setToGraph("target:graph");

        step.execute(dataset, state);

        Model targetModel = dataset.getNamedModel("target:graph");
        assertTrue(
                targetModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testAddStepWithNonExistentGraph() throws MojoExecutionException {
        AddStep step = new AddStep();
        step.getInputsComponent().addGraph("nonexistent:graph");
        step.setToGraph("target:graph");
        step.execute(dataset, state);
        // no exception
    }

    @Test
    void testShaclInferStep() throws MojoExecutionException, IOException {
        ShaclInferStep step = new ShaclInferStep();
        InputsComponent<ShaclInferStep> shapes = new InputsComponent<>(step);
        InputsComponent<ShaclInferStep> data = new InputsComponent<>(step);
        shapes.addFile(
                makeFileParameterUnderTestOutputDir("test-data/shapes.ttl").getRelativePath());
        data.addFile(makeFileParameterUnderTestOutputDir("test-data/data.ttl").getRelativePath());
        Inferred inferred = new Inferred();
        inferred.setGraph("inferred:graph");

        step.setShapes(shapes);
        step.setData(data);
        step.setInferred(inferred);

        step.execute(dataset, state);

        Model inferredModel = dataset.getNamedModel("inferred:graph");
        assertTrue(
                inferredModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/inferred"),
                        ResourceFactory.createResource("http://example.org/NewObject")));
    }

    private RelativePath makeFileParameterUnderTestOutputDir(String relativePath) {
        return state.files().makeRelativeToOutputBase(relativePath);
    }

    @Test
    void testShaclInferStepMissingInferredGraph() throws MojoExecutionException {
        ShaclInferStep step = new ShaclInferStep();
        InputsComponent<ShaclInferStep> shapes = new InputsComponent<>(step);
        InputsComponent<ShaclInferStep> data = new InputsComponent<>(step);
        shapes.addFile(
                makeFileParameterUnderTestOutputDir("test-data/shapes.ttl").getRelativePath());
        data.addFile(makeFileParameterUnderTestOutputDir("test-data/data.ttl").getRelativePath());
        Inferred inferred = new Inferred();
        step.setShapes(shapes);
        step.setData(data);
        step.setInferred(inferred);
        step.execute(dataset, state);
    }

    @Test
    void testShaclInferStepNonExistentShapesFile() {
        ShaclInferStep step = new ShaclInferStep();
        InputsComponent<ShaclInferStep> shapes = new InputsComponent<>(step);
        InputsComponent<ShaclInferStep> data = new InputsComponent<>(step);
        shapes.addFile(
                state.files()
                        .makeRelativeToOutputBase("test-data/nonexistent.ttl")
                        .getRelativePath());
        data.addFile(
                state.files().makeRelativeToOutputBase("test-data/data.ttl").getRelativePath());
        Inferred inferred = new Inferred();
        inferred.setGraph("inferred:graph");
        step.setShapes(shapes);
        step.setData(data);
        step.setInferred(inferred);

        assertThrows(
                Exception.class,
                () -> step.execute(dataset, state),
                "Should throw when shapes file is missing");
    }

    @Test
    void testShaclInferStepEmptyData() throws MojoExecutionException, IOException {
        ShaclInferStep step = new ShaclInferStep();
        InputsComponent<ShaclInferStep> shapes = new InputsComponent<>(step);
        InputsComponent<ShaclInferStep> data = new InputsComponent<>(step);
        shapes.addFile(
                state.files().makeRelativeToOutputBase("test-data/shapes.ttl").getRelativePath());
        Inferred inferred = new Inferred();
        inferred.setGraph("inferred:graph");

        String shapesTtl =
                """
                        @prefix sh: <http://www.w3.org/ns/shacl#> .
                        @prefix ex: <http://example.org/> .
                        ex:InferRule a sh:NodeShape ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:inferred ; sh:object ex:NewObject ] .""";
        RelativePath shapesFile = state.files().makeRelativeToOutputBase("test-data/shapes.ttl");
        state.files().createParentFolder(shapesFile);
        state.files().writeText(shapesFile, shapesTtl);

        step.setShapes(shapes);
        step.setData(data);
        step.setInferred(inferred);

        step.execute(dataset, state);
        Model inferredModel = dataset.getNamedModel("inferred:graph");
        assertTrue(inferredModel.isEmpty(), "Inferred model should be empty with no data");
    }

    @Test
    void testSparqlUpdateStep() throws MojoExecutionException {
        String sparql =
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }";
        SparqlUpdateStep step = new SparqlUpdateStep();
        step.setSparql(sparql);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testSparqlUpdateStepMissingSparql() {
        SparqlUpdateStep step = new SparqlUpdateStep();
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when sparql is missing");
    }

    @Test
    void testSparqlUpdateStepMalformedSparql() {
        SparqlUpdateStep step = new SparqlUpdateStep();
        step.setSparql("INVALID SPARQL");
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when sparql is malformed");
    }

    @Test
    void testSparqlUpdateStepWithVariableBindings() throws MojoExecutionException {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("fileGraph"),
                RDFIO.value,
                ResourceFactory.createResource("test:graph"));
        String sparql =
                "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}";
        SparqlUpdateStep step = new SparqlUpdateStep();
        step.setSparql(sparql);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testWriteStep() throws MojoExecutionException, IOException {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        RelativePath outputFile = state.files().makeRelativeToOutputBase("output.ttl");
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                outputFile.getRelativePathAsResource(),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));

        WriteStep step = new WriteStep();
        step.addGraph("test:graph");

        step.execute(dataset, state);

        Model writtenModel = ModelFactory.createDefaultModel();
        state.files().readRdf(outputFile, writtenModel);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testWriteStepMissingGraph() {
        WriteStep step = new WriteStep();
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when graph is missing");
    }

    @Test
    void testWriteStepNoFileMapping() {
        WriteStep step = new WriteStep();
        step.addGraph("test:graph");
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when no file mapping exists");
    }

    @Test
    void testWriteStepMultipleFileMappings() {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                ResourceFactory.createResource("file://test-data/file1.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));
        metaModel.add(
                ResourceFactory.createResource("file://test-data/file2.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));
        WriteStep step = new WriteStep();
        step.addGraph("test:graph");
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when multiple file mappings exist");
    }

    @Test
    void testWriteStepWithToFile() throws MojoExecutionException, IOException {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        RelativePath outputFile = testOutputBase.subFile("custom-output.ttl");

        WriteStep step = new WriteStep();
        step.addGraph("test:graph");
        step.setToFile(outputFile.getRelativePath());

        step.execute(dataset, state);

        Model writtenModel = ModelFactory.createDefaultModel();
        state.files().readRdf(outputFile, writtenModel);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testSavepointStep() throws MojoExecutionException, IOException {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                ResourceFactory.createResource("test-data/sample.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));

        SavepointStep step = new SavepointStep();
        step.setId("sp001");
        step.setEnabled(true);

        String hash = step.calculateHash("", state);
        step.execute(dataset, state);

        RelativePath hashPath = state.getSavepointCache().getHashFile("sp001");
        RelativePath datasetPath = state.getSavepointCache().getDatasetFile("sp001");
        assertTrue(hashPath.exists(), "hash.txt should exist");
        assertTrue(datasetPath.exists(), "dataset.trig should exist");

        String storedHash = state.files().readText(hashPath);
        assertEquals(hash, storedHash, "Stored hash should match computed hash");

        Dataset loadedDataset = DatasetFactory.create();
        state.files().readRdf(datasetPath, loadedDataset);
        Model loadedModel = loadedDataset.getNamedModel("test:graph");
        System.out.println(PipelineHelper.datasetToPrettyTrig(dataset));
        assertTrue(
                loadedModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Loaded dataset should contain original triple");

        String newHash = step.calculateHash("abc123", state);
        state.setPreviousStepHash("abc123");
        PipelineHelper.clearDataset(dataset);
        step.execute(dataset, state);
        assertTrue(dataset.isEmpty(), "Dataset should be empty after invalid savepoint");
        String newStoredHash = state.files().readText(hashPath);
        assertEquals(newHash, newStoredHash, "New stored hash should match new computed hash");
    }

    @Test
    void testSavepointStepDisabled() throws MojoExecutionException {
        SavepointStep step = new SavepointStep();
        step.setId("sp001" + System.currentTimeMillis());
        step.setEnabled(false);
        state.setAllowLoadingFromSavepoint(true);
        step.execute(dataset, state);
        assertTrue(
                state.isAllowLoadingFromSavepoint(),
                "allowLoadingFromSavepoint should remain unchanged for disabled savepoint");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
        RelativePath savepointDir = state.getSavepointCache().getSavepointDir(step.getId());
        assertFalse(savepointDir.exists(), "Savepoint directory should not be created");
    }

    @Test
    void testSavepointStepMissingId() {
        SavepointStep step = new SavepointStep();
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when savepoint id is missing");
    }

    @Test
    void testSavepointStepInvalidHashFile() throws MojoExecutionException, IOException {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        model.setNsPrefix("ex", "http://example.org/");
        SavepointStep step = new SavepointStep();
        step.setId("sp001");
        step.setEnabled(true);
        step.execute(dataset, state);

        RelativePath hashFile = state.getSavepointCache().getHashFile("sp001");
        state.files().writeText(hashFile, "invalid-hash");

        PipelineHelper.clearDataset(dataset);
        step.execute(dataset, state);
        assertTrue(dataset.isEmpty(), "Dataset should be empty after invalid hash");
    }

    @Test
    void testForeachStep() throws MojoExecutionException {
        Model model1 = ModelFactory.createDefaultModel();
        model1.add(
                ResourceFactory.createResource("http://example.org/dummy1"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        dataset.addNamedModel("vocab:test1", model1);
        Model model2 = ModelFactory.createDefaultModel();
        model2.add(
                ResourceFactory.createResource("http://example.org/dummy2"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        dataset.addNamedModel("vocab:test2", model2);

        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "PREFIX rdfio: <http://qudtlib.org/rdfio/> "
                        + "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");

        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        step.setValues(values);
        step.addBodyStep(bodyStep);

        step.execute(dataset, state);

        assertTrue(
                dataset.getNamedModel("vocab:test1")
                        .contains(
                                ResourceFactory.createResource("http://example.org/s"),
                                ResourceFactory.createProperty("http://example.org/p"),
                                ResourceFactory.createResource("http://example.org/o")));
        assertTrue(
                dataset.getNamedModel("vocab:test2")
                        .contains(
                                ResourceFactory.createResource("http://example.org/s"),
                                ResourceFactory.createProperty("http://example.org/p"),
                                ResourceFactory.createResource("http://example.org/o")));
    }

    @Test
    void testForeachStepMissingVar() {
        ForeachStep step = new ForeachStep();
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }");
        step.setValues(values);
        step.addBodyStep(bodyStep);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when var is missing");
    }

    @Test
    void testForeachStepMissingValues() {
        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }");
        step.addBodyStep(bodyStep);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when values are missing");
    }

    @Test
    void testForeachStepMissingBody() {
        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("vocab:*");
        Values values = new Values();
        values.setGraphs(graphs);
        step.setValues(values);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when body is missing");
    }

    @Test
    void testForeachStepEmptyGraphs() throws MojoExecutionException {
        GraphSelection graphs = new GraphSelection();
        graphs.addInclude("nonexistent:*");
        Values values = new Values();
        values.setGraphs(graphs);
        SparqlUpdateStep bodyStep = new SparqlUpdateStep();
        bodyStep.setSparql(
                "PREFIX rdfio: <http://qudtlib.org/rdfio/> "
                        + "INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}");

        ForeachStep step = new ForeachStep();
        step.setVar("fileGraph");
        step.setValues(values);
        step.addBodyStep(bodyStep);

        step.execute(dataset, state);
        assertTrue(dataset.isEmpty(), "Dataset should remain empty with no matching graphs");
    }

    @Test
    void testPipelineMojoMissingPipelineId() throws Exception {
        PipelineMojo mojo = new PipelineMojo();
        Pipeline pipeline = new Pipeline();
        pipeline.setSteps(new ArrayList<>());
        setField(mojo, "pipeline", pipeline);

        assertThrows(
                MojoExecutionException.class,
                mojo::execute,
                "Should throw when pipelineId is missing");
    }

    @Test
    void testPipelineMojoInvalidStepType() throws Exception {
        // Invalid step types are handled by Plexus; simulate by adding an unknown step
        PipelineMojo mojo = new PipelineMojo();
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(baseDir);
        List<Step> steps = new ArrayList<>();
        steps.add(new UnknownStep()); // Custom step to simulate invalid type
        pipeline.setSteps(steps);
        setField(mojo, "pipeline", pipeline);

        // Plexus may not instantiate unknown steps correctly, but we test configuration validation
        assertThrows(
                MojoExecutionException.class,
                mojo::execute,
                "Should throw for invalid step configuration");
    }

    @Test
    void testPipelineMojoWithSavepoints() throws Exception {
        PipelineMojo mojo = new PipelineMojo();
        mojo.setBaseDir(new File("."));
        mojo.setWorkBaseDir(new File("target"));
        setField(
                mojo,
                "configuration",
                Xpp3DomBuilder.build(
                        Files.newInputStream(Path.of("src/test/resources/pipeline-config.xml")),
                        "UTF-8"));
        setField(mojo, "baseDir", baseDir);
        mojo.parseConfiguration();
        Pipeline pipeline = mojo.getPipeline();
        pipeline.setForceRun(true);
        mojo.execute();
        pipeline.setForceRun(false);
        Dataset loadedDataset = DatasetFactory.create();
        RelativePath datasetPath = state.getSavepointCache().getDatasetFile("sp002");
        state.files().readRdf(datasetPath, loadedDataset);
        Model loadedModel = loadedDataset.getNamedModel("test:graph");
        assertTrue(
                loadedModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")));
        assertTrue(
                loadedModel.contains(
                        ResourceFactory.createResource("http://example.org/s2"),
                        ResourceFactory.createProperty("http://example.org/p2"),
                        ResourceFactory.createResource("http://example.org/o2")));

        mojo.execute();
        // we really have no way of checking whether or not the savepoint 002 was used. Let's
        // have this test as a smoke test.
    }

    @Test
    void testSparqlHelperSelectQuery() throws MojoExecutionException {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("fileGraph"),
                RDFIO.value,
                ResourceFactory.createResource("test:graph"));
        dataset.getNamedModel("test:graph")
                .add(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o"));

        StringBuilder result = new StringBuilder();
        io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.executeSparqlQueryWithVariables(
                "SELECT ?s WHERE { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper
                        .QueryResultProcessor() {
                    @Override
                    public void processSelectResult(ResultSet rs) {
                        while (rs.hasNext()) {
                            result.append(rs.next().get("s").toString());
                        }
                    }
                });

        assertEquals(
                "http://example.org/s",
                result.toString(),
                "Select query should return expected result");
    }

    @Test
    void testSparqlHelperAskQuery() throws MojoExecutionException {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("fileGraph"),
                RDFIO.value,
                ResourceFactory.createResource("test:graph"));
        dataset.getNamedModel("test:graph")
                .add(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o"));

        boolean[] result = new boolean[1];
        io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.executeSparqlQueryWithVariables(
                "ASK { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper
                        .QueryResultProcessor() {
                    @Override
                    public void processAskResult(boolean res) {
                        result[0] = res;
                    }
                });

        assertTrue(result[0], "Ask query should return true");
    }

    @Test
    void testSparqlHelperConstructQuery() throws MojoExecutionException {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("fileGraph"),
                RDFIO.value,
                ResourceFactory.createResource("test:graph"));
        dataset.getNamedModel("test:graph")
                .add(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o"));

        Model[] result = new Model[1];
        io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper.executeSparqlQueryWithVariables(
                "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper
                        .QueryResultProcessor() {
                    @Override
                    public void processConstructOrDescribeResult(Model res) {
                        result[0] = res;
                    }
                });

        assertTrue(
                result[0].contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Construct query should return expected model");
    }

    @Test
    void testSparqlHelperInvalidQuery() {
        assertThrows(
                MojoExecutionException.class,
                () ->
                        io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper
                                .executeSparqlQueryWithVariables(
                                        "INVALID QUERY",
                                        dataset,
                                        state.getMetadataGraph(),
                                        new SparqlHelper.QueryResultProcessor() {}),
                "Should throw for invalid SPARQL query");
    }

    @Test
    void testSavepointValidationFailsOnInputFileChange() throws Exception {
        // Create input file
        File inputFile = new File("target/rdfio/test-output/test-input.ttl");
        inputFile.getParentFile().mkdirs();
        String originalContent =
                "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";
        Files.write(inputFile.toPath(), originalContent.getBytes());

        // Configure pipeline
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(baseDir);
        List<Step> steps = new ArrayList<>();
        AddStep addStep = new AddStep();
        addStep.getInputsComponent().addFile("target/rdfio/test-output/test-input.ttl");
        addStep.setToGraph("test:graph");
        SavepointStep savepointStep = new SavepointStep();
        savepointStep.setId("sp001");
        savepointStep.setEnabled(true);
        steps.add(addStep);
        steps.add(savepointStep);
        pipeline.setSteps(steps);

        // First execution
        PipelineMojo mojo = new PipelineMojo();
        mojo.setBaseDir(new File("."));
        mojo.setWorkBaseDir(new File("target"));
        setField(mojo, "pipeline", pipeline);
        mojo.execute();

        // Verify savepoint created
        RelativePath datasetPath = state.getSavepointCache().getDatasetFile("sp001");
        assertTrue(datasetPath.exists(), "Savepoint dataset.trig should exist");

        // Modify input file
        String modifiedContent =
                "<http://example.org/s> <http://example.org/p> <http://example.org/o2> .";
        Files.write(inputFile.toPath(), modifiedContent.getBytes());

        // Clear dataset and re-execute
        mojo.execute();

        // Verify pipeline re-executed (dataset not loaded from savepoint)
        Model model = mojo.getDataset().getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o2")),
                "Model should contain modified triple after re-execution");
        assertFalse(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Model should not contain original triple");
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    // Dummy class to simulate an invalid step
    private static class UnknownStep implements Step {

        @Override
        public String getElementName() {
            return "unknown";
        }

        @Override
        public void execute(Dataset dataset, PipelineState state) {
            throw new UnsupportedOperationException("Unknown step");
        }

        @Override
        public String calculateHash(String previousHash, PipelineState state) {
            return previousHash;
        }
    }
}
