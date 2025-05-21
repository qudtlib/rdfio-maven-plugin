package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.FileHelper;
import java.io.File;
import java.nio.file.Files;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AddStepExecuteTests {
    private Dataset dataset;
    private PipelineState state;
    private File baseDir;
    private static final String TEST_RDF_FILE = "src/test/resources/data.ttl";
    private static final String EXPECTED_SUBJECT = "http://example.org/s";
    private static final String EXPECTED_PREDICATE = "http://example.org/p";
    private static final String EXPECTED_OBJECT = "http://example.org/o";

    @BeforeEach
    void setUp() throws Exception {
        dataset = DatasetFactory.create();
        baseDir = new File(System.getProperty("user.dir"));
        state =
                new PipelineState(
                        "test-pipeline",
                        RDFIO.metadataGraph.toString(),
                        baseDir,
                        new File("target/rdfio/test-output"));
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
    void testExecuteLoadsFileToTargetGraph() throws Exception {
        // Arrange: Configure AddStep with a file and target graph
        String xmlConfig =
                """
                <add>
                    <file>%s</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """
                        .formatted(TEST_RDF_FILE);
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);
        AddStep.parse(config);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify the data is loaded into the target graph
        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Target graph should contain the triple from the input file");

        // Verify metadata
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        assertTrue(
                metaModel.contains(
                        FileHelper.getFileUrl(
                                FileHelper.resolveRelativeUnixPath(baseDir, TEST_RDF_FILE)),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource("test:graph")),
                "Metadata should map the file to the target graph");
    }

    @Test
    void testExecuteCopiesGraphToTargetGraph() throws Exception {
        // Arrange: Populate a source graph
        Model sourceModel = dataset.getNamedModel("source:graph");
        sourceModel.add(
                ResourceFactory.createResource(EXPECTED_SUBJECT),
                ResourceFactory.createProperty(EXPECTED_PREDICATE),
                ResourceFactory.createResource(EXPECTED_OBJECT));

        String xmlConfig =
                """
                <add>
                    <graph>source:graph</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify the data is copied to the target graph
        Model targetModel = dataset.getNamedModel("test:graph");
        assertTrue(
                targetModel.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Target graph should contain the triple from the source graph");

        // Verify metadata (no file mapping for graph-to-graph copy)
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        assertFalse(
                metaModel.contains(
                        null, RDFIO.loadsInto, ResourceFactory.createResource("test:graph")),
                "Metadata should not contain file mappings for graph copy");
    }

    @Test
    void testExecuteMultipleFilesToTargetGraph() throws Exception {
        // Arrange: Create a second test file
        File secondRdfFile = new File(baseDir, "src/test/resources/data2.ttl");
        Files.createDirectories(secondRdfFile.getParentFile().toPath());
        String secondContent =
                "<http://example.org/s2> <http://example.org/p2> <http://example.org/o2> .";
        Files.write(secondRdfFile.toPath(), secondContent.getBytes());

        String xmlConfig =
                """
                <add>
                    <file>%s</file>
                    <file>src/test/resources/data2.ttl</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """
                        .formatted(TEST_RDF_FILE);
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify both triples are in the target graph
        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Target graph should contain the triple from the first file");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s2"),
                        ResourceFactory.createProperty("http://example.org/p2"),
                        ResourceFactory.createResource("http://example.org/o2")),
                "Target graph should contain the triple from the second file");

        // Verify metadata
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        assertTrue(
                metaModel.contains(
                        FileHelper.getFileUrl(
                                FileHelper.resolveRelativeUnixPath(baseDir, TEST_RDF_FILE)),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource("test:graph")),
                "Metadata should map the first file to the target graph");
        assertTrue(
                metaModel.contains(
                        FileHelper.getFileUrl(secondRdfFile),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource("test:graph")),
                "Metadata should map the second file to the target graph");
    }

    @Test
    void testExecuteMissingToGraph() throws Exception {
        // Arrange: Configure AddStep without toGraph
        String xmlConfig =
                """
                <add>
                    <file>%s</file>
                </add>
                """
                        .formatted(TEST_RDF_FILE);
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));

        // Act & Assert: Expect exception
        assertThrows(
                ConfigurationParseException.class,
                () -> {
                    AddStep step = AddStep.parse(config);
                    step.execute(dataset, state);
                },
                "Should throw when toGraph is missing");
    }

    @Test
    void testExecuteNoFilesOrGraphs() throws Exception {
        // Arrange: Configure AddStep with only toGraph
        String xmlConfig =
                """
                <add>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));

        // Act & Assert: Expect exception
        assertThrows(
                ConfigurationParseException.class,
                () -> {
                    AddStep step = AddStep.parse(config);
                    step.execute(dataset, state);
                },
                "Should throw when neither files nor graphs are specified");
    }

    @Test
    void testExecuteNonExistentFile() throws Exception {
        // Arrange: Configure AddStep with a non-existent file
        String xmlConfig =
                """
                <add>
                    <file>src/test/resources/nonexistent.ttl</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act & Assert: Expect exception
        assertThrows(
                PluginConfigurationExeception.class,
                () -> step.execute(dataset, state),
                "Should throw when the specified file does not exist");
    }

    @Test
    void testExecuteNonExistentSourceGraph() throws Exception {
        // Arrange: Configure AddStep with a non-existent source graph
        String xmlConfig =
                """
                <add>
                    <graph>nonexistent:graph</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act & Assert: Expect exception
        assertThrows(
                PluginConfigurationExeception.class,
                () -> step.execute(dataset, state),
                "Should throw when the source graph does not exist");
    }

    @Test
    void testExecuteFileAndGraphTogether() throws Exception {
        // Arrange: Populate a source graph
        Model sourceModel = dataset.getNamedModel("source:graph");
        sourceModel.add(
                ResourceFactory.createResource("http://example.org/s2"),
                ResourceFactory.createProperty("http://example.org/p2"),
                ResourceFactory.createResource("http://example.org/o2"));

        String xmlConfig =
                """
                <add>
                    <file>%s</file>
                    <graph>source:graph</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """
                        .formatted(TEST_RDF_FILE);
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify both triples are in the target graph
        Model targetModel = dataset.getNamedModel("test:graph");
        assertTrue(
                targetModel.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Target graph should contain the triple from the file");
        assertTrue(
                targetModel.contains(
                        ResourceFactory.createResource("http://example.org/s2"),
                        ResourceFactory.createProperty("http://example.org/p2"),
                        ResourceFactory.createResource("http://example.org/o2")),
                "Target graph should contain the triple from the source graph");

        // Verify metadata
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        assertTrue(
                metaModel.contains(
                        FileHelper.getFileUrl(
                                FileHelper.resolveRelativeUnixPath(baseDir, TEST_RDF_FILE)),
                        RDFIO.loadsInto,
                        ResourceFactory.createResource("test:graph")),
                "Metadata should map the file to the target graph");
    }

    @Test
    void testExecutePreservesExistingTargetGraphData() throws Exception {
        // Arrange: Populate the target graph with existing data
        Model targetModel = dataset.getNamedModel("test:graph");
        targetModel.add(
                ResourceFactory.createResource("http://example.org/existing"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        String xmlConfig =
                """
                <add>
                    <file>%s</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """
                        .formatted(TEST_RDF_FILE);
        Xpp3Dom config = Xpp3DomBuilder.build(new java.io.StringReader(xmlConfig));
        AddStep step = AddStep.parse(config);

        // Act: Execute the step
        step.execute(dataset, state);

        // Assert: Verify both existing and new triples are in the target graph
        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/existing"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should retain the existing triple");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource(EXPECTED_SUBJECT),
                        ResourceFactory.createProperty(EXPECTED_PREDICATE),
                        ResourceFactory.createResource(EXPECTED_OBJECT)),
                "Target graph should contain the new triple from the file");
    }
}
