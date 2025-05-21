package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShaclInferStepExecuteTests {
    private Dataset dataset;
    private PipelineState state;
    private File baseDir;
    private File workBaseDir;
    private File testOutputBase;
    private String pipelineId;

    @BeforeEach
    void setUp() {
        dataset = DatasetFactory.create();
        baseDir = new File(".");
        workBaseDir = new File("target");
        baseDir.mkdirs();
        workBaseDir.mkdirs();
        pipelineId = "test-pipeline";
        state = new PipelineState(pipelineId, baseDir, workBaseDir, null, null, null);
        testOutputBase = new File(workBaseDir, "test-output");
        testOutputBase.mkdirs();
    }

    @Test
    void testShaclInferStepWithValidShapesAndData() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>test:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);

        // Setup dataset with data
        Model dataModel = dataset.getNamedModel("test:data");
        dataModel.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        // Setup shapes file
        File shapesFile = new File("src/test/resources/shapes.ttl");
        assertTrue(shapesFile.exists(), "Shapes file must exist");

        step.execute(dataset, state);

        // Verify inferred file
        File outputFile = new File(testOutputBase, "inferred.ttl");
        assertTrue(outputFile.exists(), "Inferred file should be created");
        Model inferredModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(inferredModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                inferredModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/inferred"),
                        ResourceFactory.createResource("http://example.org/NewObject")),
                "Inferred model should contain the expected triple");
    }

    @Test
    void testShaclInferStepWithMissingShapesFile() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>nonexistent/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>test:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);

        // Setup dataset
        Model dataModel = dataset.getNamedModel("test:data");
        dataModel.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when shapes file does not exist");
    }

    @Test
    void testShaclInferStepWithNonExistentDataGraph() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>nonexistent:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when data graph does not exist");
    }

    @Test
    void testShaclInferStepWithEmptyDataGraph() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>test:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);
        // Setup empty data graph
        dataset.getNamedModel("test:data");
        assertThrows(MojoExecutionException.class, () -> step.execute(dataset, state));
    }

    @Test
    void testShaclInferStepWithMultipleShapesAndData() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                        <graph>shacl:shapes</graph>
                    </shapes>
                    <data>
                        <graph>test:data1</graph>
                        <graph>test:data2</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);

        // Setup dataset with data
        Model dataModel1 = dataset.getNamedModel("test:data1");
        dataModel1.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        Model dataModel2 = dataset.getNamedModel("test:data2");
        dataModel2.add(
                ResourceFactory.createResource("http://example.org/s2"),
                ResourceFactory.createProperty("http://example.org/p2"),
                ResourceFactory.createResource("http://example.org/o2"));
        Model shapesModel = dataset.getNamedModel("shacl:shapes");
        RDFDataMgr.read(
                shapesModel, new FileInputStream("src/test/resources/shapes.ttl"), Lang.TTL);

        step.execute(dataset, state);

        // Verify inferred file
        File outputFile = new File(testOutputBase, "inferred.ttl");
        assertTrue(outputFile.exists(), "Inferred file should be created");
        Model inferredModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(inferredModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                inferredModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/inferred"),
                        ResourceFactory.createResource("http://example.org/NewObject")),
                "Inferred model should contain the expected triple from data1");
    }

    @Test
    void testShaclInferStepWithInvalidOutputPath() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>test:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>/invalid/path/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);

        // Setup dataset
        Model dataModel = dataset.getNamedModel("test:data");
        dataModel.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when writing to an invalid file path");
    }

    @Test
    void testShaclInferStepWithRepeatUntilStable() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <repeatUntilStable>true</repeatUntilStable>
                    <shapes>
                        <file>src/test/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <graph>test:data</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/test-output/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = ShaclInferStep.parse(config);

        // Setup dataset
        Model dataModel = dataset.getNamedModel("test:data");
        dataModel.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        step.execute(dataset, state);

        // Verify inferred file
        File outputFile = new File(testOutputBase, "inferred.ttl");
        assertTrue(outputFile.exists(), "Inferred file should be created");
        Model inferredModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(inferredModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                inferredModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/inferred"),
                        ResourceFactory.createResource("http://example.org/NewObject")),
                "Inferred model should contain the expected triple");
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }
}
