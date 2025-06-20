package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.pipeline.step.AddStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.Step;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class AddStepParseTests {

    @Test
    void testParseAddStepSingleFile() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepMultipleFiles() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input1.ttl</file>
                    <file>target/test-input2.ttl</file>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(2, step.getInputsComponent().getFiles().size(), "Should have two files");
        assertEquals(
                "target/test-input1.ttl",
                step.getInputsComponent().getFiles().get(0),
                "First file path should match");
        assertEquals(
                "target/test-input2.ttl",
                step.getInputsComponent().getFiles().get(1),
                "Second file path should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepFileSelection() throws Exception {
        String xml =
                """
                <add>
                    <files>
                        <include>target/*.ttl</include>
                        <exclude>target/temp/*.ttl</exclude>
                    </files>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertTrue(step.getInputsComponent().getFiles().isEmpty(), "Files should be empty");
        assertNotNull(
                step.getInputsComponent().getFileSelection(), "FileSelection should not be null");
        assertEquals(
                1,
                step.getInputsComponent().getFileSelection().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "target/*.ttl",
                step.getInputsComponent().getFileSelection().getInclude().get(0),
                "Include pattern should match");
        assertEquals(
                1,
                step.getInputsComponent().getFileSelection().getExclude().size(),
                "Should have one exclude pattern");
        assertEquals(
                "target/temp/*.ttl",
                step.getInputsComponent().getFileSelection().getExclude().get(0),
                "Exclude pattern should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepSingleGraph() throws Exception {
        String xml =
                """
                <add>
                    <graph>source:graph</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertTrue(step.getInputsComponent().getFiles().isEmpty(), "Files should be empty");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertEquals(1, step.getInputsComponent().getGraphs().size(), "Should have one graph");
        assertEquals(
                "source:graph", step.getInputsComponent().getGraphs().get(0), "Graph should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepMultipleGraphs() throws Exception {
        String xml =
                """
                <add>
                    <graph>source:graph1</graph>
                    <graph>source:graph2</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertTrue(step.getInputsComponent().getFiles().isEmpty(), "Files should be empty");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertEquals(2, step.getInputsComponent().getGraphs().size(), "Should have two graphs");
        assertEquals(
                "source:graph1",
                step.getInputsComponent().getGraphs().get(0),
                "First graph should match");
        assertEquals(
                "source:graph2",
                step.getInputsComponent().getGraphs().get(1),
                "Second graph should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraphsPattern>test:graph_{1}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraph(), "toGraph should be null");
        assertEquals("test:graph_{1}", step.getToGraphsPattern(), "toGraphsPattern should match");
    }

    @Test
    void testParseAddStepMixedInputs() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input1.ttl</file>
                    <files>
                        <include>target/*.ttl</include>
                    </files>
                    <graph>source:graph</graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input1.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNotNull(
                step.getInputsComponent().getFileSelection(), "FileSelection should not be null");
        assertEquals(
                1,
                step.getInputsComponent().getFileSelection().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "target/*.ttl",
                step.getInputsComponent().getFileSelection().getInclude().get(0),
                "Include pattern should match");
        assertEquals(1, step.getInputsComponent().getGraphs().size(), "Should have one graph");
        assertEquals(
                "source:graph", step.getInputsComponent().getGraphs().get(0), "Graph should match");
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    @Test
    void testParseAddStepEmptyConfig() throws Exception {
        String xml = """
                <add/>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        assertThrows(
                Exception.class,
                () -> parseAddStep(config),
                "Empty AddStep config should throw due to missing input and missing ougput");
    }

    @Test
    void testParseAddStepMissingToGraphAndPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);
        assertEquals(
                List.of("target/test-input.ttl"),
                step.getInputsComponent().getFiles(),
                "files should match");
    }

    @Test
    void testParseAddStepEmptyFile() throws Exception {
        String xml =
                """
                <add>
                    <file></file>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        assertThrows(ConfigurationParseException.class, () -> parseAddStep(config));
    }

    @Test
    void testParseAddStepEmptyFilesElement() throws Exception {
        String xml =
                """
                <add>
                    <files/>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        assertThrows(
                Exception.class,
                () -> parseAddStep(config),
                "Empty files element should throw due to missing include patterns");
    }

    @Test
    void testParseAddStepEmptyGraph() throws Exception {
        String xml =
                """
                <add>
                    <graph></graph>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        assertThrows(ConfigurationParseException.class, () -> parseAddStep(config));
    }

    @Test
    void testParseAddStepBothToGraphAndToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraph>test:graph</toGraph>
                    <toGraphsPattern>test:graph_{1}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertEquals("test:graph", step.getToGraph(), "toGraph should take precedence");
        assertNull(
                step.getToGraphsPattern(),
                "toGraphsPattern should be ignored when toGraph is present");
    }

    @Test
    void testParseAddStepFileVarInToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraphsPattern>test:graph${name}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraph(), "toGraph should take precedence");
        assertEquals(
                step.getToGraphsPattern(),
                "test:graph${name}",
                "toGraphsPattern should be present");
    }

    @Test
    void testParseAddStepPathVarInToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraphsPattern>fromFile:${path}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraph(), "toGraph should take precedence");
        assertEquals(
                step.getToGraphsPattern(), "fromFile:${path}", "toGraphsPattern should be present");
    }

    @Test
    void testParseAddStepIndexVarInToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraphsPattern>graph:file-${index}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should match");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertNull(step.getToGraph(), "toGraph should take precedence");
        assertEquals(
                step.getToGraphsPattern(),
                "graph:file-${index}",
                "toGraphsPattern should be present");
    }

    @Test
    void testParseAddStepUnknownVarInToGraphsPattern() throws Exception {
        String xml =
                """
                <add>
                    <file>target/test-input.ttl</file>
                    <toGraphsPattern>graph:file-${something}</toGraphsPattern>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        assertThrows(ConfigurationParseException.class, () -> parseAddStep(config));
    }

    @Test
    void testParseAddStepNoInputs() throws Exception {
        String xml =
                """
                <add>
                    <toGraph>test:graph</toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);
        assertEquals("test:graph", step.getToGraph(), "toGraph should match");
    }

    @Test
    void testParseAddStepWhitespaceValues() throws Exception {
        String xml =
                """
                <add>
                    <file>  target/test-input.ttl  </file>
                    <toGraph>  test:graph  </toGraph>
                </add>
                """;
        Xpp3Dom config =
                Xpp3DomBuilder.build(
                        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8.name());
        AddStep step = parseAddStep(config);

        assertEquals(1, step.getInputsComponent().getFiles().size(), "Should have one file");
        assertEquals(
                "target/test-input.ttl",
                step.getInputsComponent().getFiles().get(0),
                "File path should be trimmed");
        assertNull(step.getInputsComponent().getFileSelection(), "FileSelection should be null");
        assertTrue(step.getInputsComponent().getGraphs().isEmpty(), "Graphs should be empty");
        assertEquals("test:graph", step.getToGraph(), "toGraph should be trimmed");
        assertNull(step.getToGraphsPattern(), "toGraphsPattern should be null");
    }

    private AddStep parseAddStep(Xpp3Dom config) throws Exception {
        Pipeline pipeline = loadPipelineConfig(config);
        return (AddStep) pipeline.getSteps().get(0);
    }

    private Pipeline loadPipelineConfig(Xpp3Dom config) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(new File("."));
        List<Step> steps = new ArrayList<>();
        steps.add(AddStep.parse(config));
        pipeline.setSteps(steps);
        return pipeline;
    }
}
