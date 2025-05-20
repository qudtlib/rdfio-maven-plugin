package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipelineStepsTests {
    private Dataset dataset;
    private PipelineState state;
    private File testOutputBase;
    private File baseDir;
    private File workBaseDir;
    private String pipelineId;

    @BeforeEach
    void setUp() {
        dataset = DatasetFactory.create();
        baseDir = new File(".");
        workBaseDir = new File("target");
        baseDir.mkdirs();
        workBaseDir.mkdirs();
        pipelineId = "test-pipeline";
        state = new PipelineState(pipelineId, RDFIO.metadataGraph.toString(), baseDir, workBaseDir);
        testOutputBase = new File(workBaseDir, "test-output");
        testOutputBase.mkdirs();
    }

    @Test
    void testAddStepWithFile() throws MojoExecutionException, IOException {
        AddStep step = new AddStep();
        String fileArg = "target/test-output/input.ttl";
        step.addFile(fileArg);
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
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        System.out.println(PipelineHelper.datasetToPrettyTrig(dataset));
        Resource fileRes =
                FileHelper.getFileUrl(FileHelper.resolveRelativeUnixPath(baseDir, fileArg));
        assertTrue(
                metaModel.contains(
                        fileRes, RDFIO.loadsInto, ResourceFactory.createResource("test:graph")));
    }

    @Test
    void testAddStepWithNonExistentFile() {
        AddStep step = new AddStep();
        step.addFile("target/rdfio/test-data/nonexistent.ttl");
        step.setToGraph("test:graph");

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when loading non-existent file");
    }

    @Test
    void testAddStepWithMissingToGraph() {
        AddStep step = new AddStep();
        step.addFile("src/test/resources/data.ttl");

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when toGraph and toGraphsPattern are missing");
    }

    @Test
    void testAddStepWithEmptyFiles() throws MojoExecutionException {
        AddStep step = new AddStep();
        FileSelection files = new FileSelection();
        files.setInclude("src/test/resources/dontfind/*.ttl");
        step.setFileSelection(files);
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
        step.addGraph("source:graph");
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
    void testAddStepWithNonExistentGraph() {
        AddStep step = new AddStep();
        step.addGraph("nonexistent:graph");
        step.setToGraph("target:graph");

        assertThrows(MojoExecutionException.class, () -> step.execute(dataset, state));
    }

    @Test
    void testShaclInferStep() throws MojoExecutionException, IOException {
        Shapes shapes = new Shapes();
        shapes.addFile(makeFileParameterUnderTestOutputDir("test-data/shapes.ttl"));
        Data data = new Data();
        data.addFile(makeFileParameterUnderTestOutputDir("test-data/data.ttl"));
        Inferred inferred = new Inferred();
        inferred.setGraph("inferred:graph");

        String shapesTtl =
                """
                        @prefix sh: <http://www.w3.org/ns/shacl#> .
                        @prefix ex: <http://example.org/> .
                        ex:InferRule a sh:NodeShape ; sh:targetNode ex:s ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:inferred ; sh:object ex:NewObject ] .""";
        String dataTtl = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";

        File shapesFile =
                new File(baseDir, makeFileParameterUnderTestOutputDir("test-data/shapes.ttl"));
        File dataFile =
                new File(baseDir, makeFileParameterUnderTestOutputDir("test-data/data.ttl"));
        shapesFile.getParentFile().mkdirs();
        Files.write(shapesFile.toPath(), shapesTtl.getBytes());
        Files.write(dataFile.toPath(), dataTtl.getBytes());

        ShaclInferStep step = new ShaclInferStep();
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

    private String makeFileParameterUnderTestOutputDir(String relativePath) {
        return testOutputBase.getPath().replace('\\', '/') + relativePath;
    }

    @Test
    void testShaclInferStepMissingInferredGraph() {
        ShaclInferStep step = new ShaclInferStep();
        Shapes shapes = new Shapes();
        shapes.addFile(makeFileParameterUnderTestOutputDir("test-data/shapes.ttl"));
        Data data = new Data();
        data.addFile(makeFileParameterUnderTestOutputDir("test-data/data.ttl"));
        Inferred inferred = new Inferred();
        step.setShapes(shapes);
        step.setData(data);
        step.setInferred(inferred);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when inferred graph is missing");
    }

    @Test
    void testShaclInferStepNonExistentShapesFile() {
        ShaclInferStep step = new ShaclInferStep();
        Shapes shapes = new Shapes();
        shapes.addFile(makeFileParameterUnderTestOutputDir("test-data/nonexistent.ttl"));
        Data data = new Data();
        data.addFile(makeFileParameterUnderTestOutputDir("test-data/data.ttl"));
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
        Shapes shapes = new Shapes();
        shapes.addFile(makeFileParameterUnderTestOutputDir("test-data/shapes.ttl"));
        Data data = new Data();
        Inferred inferred = new Inferred();
        inferred.setGraph("inferred:graph");

        String shapesTtl =
                """
                        @prefix sh: <http://www.w3.org/ns/shacl#> .
                        @prefix ex: <http://example.org/> .
                        ex:InferRule a sh:NodeShape ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:inferred ; sh:object ex:NewObject ] .""";
        File shapesFile =
                new File(baseDir, makeFileParameterUnderTestOutputDir("test-data/shapes.ttl"));
        shapesFile.getParentFile().mkdirs();
        Files.write(shapesFile.toPath(), shapesTtl.getBytes());

        ShaclInferStep step = new ShaclInferStep();
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
        File outputFile = new File(testOutputBase, "output.ttl");
        String relativePath =
                baseDir.toPath()
                        .relativize(outputFile.toPath())
                        .toString()
                        .replace(File.separator, "/");
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                ResourceFactory.createResource("file://" + relativePath),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));

        WriteStep step = new WriteStep();
        step.addGraph("test:graph");

        step.execute(dataset, state);

        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
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
        File outputFile = new File(testOutputBase, "custom-output.ttl");

        WriteStep step = new WriteStep();
        step.addGraph("test:graph");
        step.setToFile(outputFile.getPath());

        step.execute(dataset, state);

        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
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
                ResourceFactory.createResource("file://test-data/sample.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));

        SavepointStep step = new SavepointStep();
        step.setId("sp001");
        step.setEnabled(true);

        String hash = step.calculateHash("", state);
        step.execute(dataset, state);

        File savepointDir = state.getSavepointCache().getSavepointDir("sp001");
        File hashFile = new File(savepointDir, "hash.txt");
        File datasetFile = new File(savepointDir, "dataset.trig");
        assertTrue(hashFile.exists(), "hash.txt should exist");
        assertTrue(datasetFile.exists(), "dataset.trig should exist");

        String storedHash = Files.readString(hashFile.toPath(), StandardCharsets.UTF_8).trim();
        assertEquals(hash, storedHash, "Stored hash should match computed hash");

        Dataset loadedDataset = DatasetFactory.create();
        RDFDataMgr.read(loadedDataset, new FileInputStream(datasetFile), Lang.TRIG);
        Model loadedModel = loadedDataset.getNamedModel("test:graph");
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
        String newStoredHash = Files.readString(hashFile.toPath(), StandardCharsets.UTF_8).trim();
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
        File savepointDir = state.getSavepointCache().getSavepointDir(step.getId());
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
        SavepointStep step = new SavepointStep();
        step.setId("sp001");
        step.setEnabled(true);

        step.execute(dataset, state);

        File hashFile = new File(state.getSavepointCache().getSavepointDir("sp001"), "hash.txt");
        Files.write(hashFile.toPath(), "invalid-hash".getBytes());

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
        step.setBody(bodyStep);

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
        step.setBody(bodyStep);

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
        step.setBody(bodyStep);

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
        step.setBody(bodyStep);

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
        File datasetFile =
                new File(state.getSavepointCache().getSavepointDir("sp002"), "dataset.trig");
        RDFDataMgr.read(loadedDataset, new FileInputStream(datasetFile), Lang.TRIG);
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
        SparqlHelper.executeSparqlQueryWithVariables(
                "SELECT ?s WHERE { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new SparqlHelper.QueryResultProcessor() {
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
        SparqlHelper.executeSparqlQueryWithVariables(
                "ASK { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new SparqlHelper.QueryResultProcessor() {
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
        SparqlHelper.executeSparqlQueryWithVariables(
                "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?fileGraph { ?s ?p ?o } }",
                dataset,
                state.getMetadataGraph(),
                new SparqlHelper.QueryResultProcessor() {
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
                        SparqlHelper.executeSparqlQueryWithVariables(
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
        addStep.addFile("target/rdfio/test-output/test-input.ttl");
        addStep.setToGraph("test:graph");
        SavepointStep savepointStep = new SavepointStep();
        savepointStep.setId("sp001");
        savepointStep.setEnabled(true);
        steps.add(addStep);
        steps.add(savepointStep);
        pipeline.setSteps(steps);

        // First execution
        PipelineMojo mojo = new PipelineMojo();
        setField(mojo, "pipeline", pipeline);
        mojo.execute();

        // Verify savepoint created
        File savepointDir = state.getSavepointCache().getSavepointDir("sp001");
        File datasetFile = new File(savepointDir, "dataset.trig");
        assertTrue(datasetFile.exists(), "Savepoint dataset.trig should exist");

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
        public void execute(Dataset dataset, PipelineState state) {
            throw new UnsupportedOperationException("Unknown step");
        }

        @Override
        public String calculateHash(String previousHash, PipelineState state) {
            return previousHash;
        }
    }
}
