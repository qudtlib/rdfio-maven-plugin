package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

public class WriteStepExecuteTests {
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
        state = new PipelineState(pipelineId, RDFIO.metadataGraph.toString(), baseDir, workBaseDir);
        testOutputBase = new File(workBaseDir, "test-output");
        testOutputBase.mkdirs();
    }

    @Test
    void testWriteStepWithValidFileMapping() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset and metadata
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

        step.execute(dataset, state);

        // Verify written file
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain the expected triple");
    }

    @Test
    void testWriteStepWithExplicitToFile() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/test-output/custom-output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        step.execute(dataset, state);

        // Verify written file
        File outputFile = new File(testOutputBase, "custom-output.ttl");
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain the expected triple");
    }

    @Test
    void testWriteStepMissingGraph() throws Exception {
        String xml =
                """
                <write>
                    <toFile>target/test-output/output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> WriteStep.parse(config),
                "Should throw when graph is missing during parsing");
    }

    @Test
    void testWriteStepEmptyGraph() throws Exception {
        String xml =
                """
                <write>
                    <graph></graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> WriteStep.parse(config),
                "Should throw when graph is empty during parsing");
    }

    @Test
    void testWriteStepNonExistentGraph() throws Exception {
        String xml =
                """
                <write>
                    <graph>nonexistent:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // No metadata or graph exists
        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when graph does not exist and no file mapping is found");
    }

    @Test
    void testWriteStepNoFileMapping() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset but no metadata
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when no file mapping exists");
    }

    @Test
    void testWriteStepMultipleFileMappings() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset and multiple metadata mappings
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                ResourceFactory.createResource("file://test-data/file1.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));
        metaModel.add(
                ResourceFactory.createResource("file://test-data/file2.ttl"),
                RDFIO.loadsInto,
                ResourceFactory.createResource("test:graph"));

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when multiple file mappings exist");
    }

    @Test
    void testWriteStepEmptyModel() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup empty graph and metadata
        dataset.getNamedModel("test:graph"); // Create empty model
        File outputFile = new File(testOutputBase, "empty-output.ttl");
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

        step.execute(dataset, state);

        // Verify written file is empty
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(writtenModel.isEmpty(), "Written model should be empty");
    }

    @Test
    void testWriteStepToFileDirectoryCreation() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/test-output/new-dir/output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        step.execute(dataset, state);

        // Verify written file and directory
        File outputFile = new File(testOutputBase, "new-dir/output.ttl");
        assertTrue(outputFile.exists(), "Output file should be created");
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain the expected triple");
    }

    @Test
    void testWriteStepInvalidFilePath() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>/invalid/path/output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when writing to an invalid file path");
    }

    @Test
    void testWriteStepOverwriteExistingFile() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/test-output/overwrite.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Create existing file with different content
        File outputFile = new File(testOutputBase, "overwrite.ttl");
        Files.write(
                outputFile.toPath(),
                "<http://example.org/old> <http://example.org/p> <http://example.org/o> ."
                        .getBytes());

        // Setup dataset
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        step.execute(dataset, state);

        // Verify file overwritten
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain new triple");
        assertFalse(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/old"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should not contain old triple");
    }

    @Test
    void testWriteStepWithDefaultGraph() throws Exception {
        String xml =
                """
                <write>
                    <graph>DEFAULT</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup default graph and metadata
        Model model = dataset.getNamedModel("DEFAULT");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));
        File outputFile = new File(testOutputBase, "default-output.ttl");
        String relativePath =
                baseDir.toPath()
                        .relativize(outputFile.toPath())
                        .toString()
                        .replace(File.separator, "/");
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                ResourceFactory.createResource("file://" + relativePath),
                RDFIO.loadsInto,
                ResourceFactory.createResource("DEFAULT"));

        step.execute(dataset, state);

        // Verify written file
        Model writtenModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(writtenModel, new FileInputStream(outputFile), Lang.TTL);
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain the expected triple");
    }

    @Test
    void testWriteStepWithQuadsFormat() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/test-output/output.trig</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        // Setup dataset with named graph
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        step.execute(dataset, state);

        // Verify written file in TRIG format
        Dataset writtenDataset = DatasetFactory.create();
        RDFDataMgr.read(
                writtenDataset,
                new FileInputStream(new File(testOutputBase, "output.trig")),
                Lang.TRIG);
        Model writtenModel = writtenDataset.getNamedModel("test:graph");
        assertTrue(
                writtenModel.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Written model should contain the expected triple");
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }
}
