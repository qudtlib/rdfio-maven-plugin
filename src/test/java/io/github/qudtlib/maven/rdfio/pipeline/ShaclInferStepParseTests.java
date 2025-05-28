package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.pipeline.step.ShaclInferStep;
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

public class ShaclInferStepParseTests {

    @Test
    void testParseShaclInferStepMinimalValid() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);

        assertEquals(1, step.getShapes().getFiles().size(), "Should have one shape file");
        assertEquals(
                "shapes.ttl", step.getShapes().getFiles().get(0), "Shape file path should match");
        assertNull(step.getShapes().getFileSelection(), "Shape FileSelection should be null");
        assertTrue(step.getShapes().getGraphs().isEmpty(), "Shape graphs should be empty");
        assertNull(step.getShapes().getGraphSelection(), "Shape GraphSelection should be null");
        assertEquals(1, step.getData().getFiles().size(), "Should have one data file");
        assertEquals("data.ttl", step.getData().getFiles().get(0), "Data file path should match");
        assertNull(step.getData().getFileSelection(), "Data FileSelection should be null");
        assertTrue(step.getData().getGraphs().isEmpty(), "Data graphs should be empty");
        assertNull(step.getData().getGraphSelection(), "Data GraphSelection should be null");
        assertEquals(
                "inferred:graph", step.getInferred().getGraph(), "Inferred graph should match");
        assertNull(step.getInferred().getFile(), "Inferred file should be null");
        assertFalse(step.isIterateUntilStable(), "iterateUntilStable should be false by default");
        assertNull(step.getMessage(), "Message should be null");
    }

    @Test
    void testParseShaclInferStepFullConfig() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <message>Inferring triples</message>
                    <shapes>
                        <file>shapes1.ttl</file>
                        <file>shapes2.ttl</file>
                        <files>
                            <include>shapes/*.ttl</include>
                            <exclude>shapes/temp/*.ttl</exclude>
                        </files>
                        <graph>shapes:graph1</graph>
                        <graphs>
                            <include>shapes:*</include>
                        </graphs>
                    </shapes>
                    <data>
                        <files>
                            <include>data/*.ttl</include>
                        </files>
                        <graph>data:graph</graph>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                        <file>target/inferred.ttl</file>
                    </inferred>
                    <iterateUntilStable>true</iterateUntilStable>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);

        assertEquals("Inferring triples", step.getMessage(), "Message should match");
        assertEquals(2, step.getShapes().getFiles().size(), "Should have two shape files");
        assertEquals(
                "shapes1.ttl", step.getShapes().getFiles().get(0), "First shape file should match");
        assertEquals(
                "shapes2.ttl",
                step.getShapes().getFiles().get(1),
                "Second shape file should match");
        assertNotNull(
                step.getShapes().getFileSelection(), "Shape FileSelection should not be null");
        assertEquals(
                1,
                step.getShapes().getFileSelection().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "shapes/*.ttl",
                step.getShapes().getFileSelection().getInclude().get(0),
                "Shape include pattern should match");
        assertEquals(
                1,
                step.getShapes().getFileSelection().getExclude().size(),
                "Should have one exclude pattern");
        assertEquals(
                "shapes/temp/*.ttl",
                step.getShapes().getFileSelection().getExclude().get(0),
                "Shape exclude pattern should match");
        assertEquals(1, step.getShapes().getGraphs().size(), "Should have one shape graph");
        assertEquals(
                "shapes:graph1", step.getShapes().getGraphs().get(0), "Shape graph should match");
        assertNotNull(
                step.getShapes().getGraphSelection(), "Shape GraphSelection should not be null");
        assertEquals(
                1,
                step.getShapes().getGraphSelection().getInclude().size(),
                "Should have one shape graph include");
        assertEquals(
                "shapes:*",
                step.getShapes().getGraphSelection().getInclude().get(0),
                "Shape graph include should match");
        assertNotNull(step.getData(), "Data should not be null");
        assertTrue(step.getData().getFiles().isEmpty(), "Data files should be empty");
        assertNotNull(step.getData().getFileSelection(), "Data FileSelection should not be null");
        assertEquals(
                1,
                step.getData().getFileSelection().getInclude().size(),
                "Should have one data include pattern");
        assertEquals(
                "data/*.ttl",
                step.getData().getFileSelection().getInclude().get(0),
                "Data include pattern should match");
        assertEquals(1, step.getData().getGraphs().size(), "Should have one data graph");
        assertEquals("data:graph", step.getData().getGraphs().get(0), "Data graph should match");
        assertEquals(
                "inferred:graph", step.getInferred().getGraph(), "Inferred graph should match");
        assertEquals(
                "target/inferred.ttl", step.getInferred().getFile(), "Inferred file should match");
        assertTrue(step.isIterateUntilStable(), "iterateUntilStable should be true");
    }

    @Test
    void testParseShaclInferStepWithGraphShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <graph>shapes:graph</graph>
                    </shapes>
                    <data>
                        <graph>data:graph</graph>
                    </data>
                    <inferred>
                        <file>target/inferred.ttl</file>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);

        assertTrue(step.getShapes().getFiles().isEmpty(), "Shape files should be empty");
        assertNull(step.getShapes().getFileSelection(), "Shape FileSelection should be null");
        assertEquals(1, step.getShapes().getGraphs().size(), "Should have one shape graph");
        assertEquals(
                "shapes:graph", step.getShapes().getGraphs().get(0), "Shape graph should match");
        assertNull(step.getShapes().getGraphSelection(), "Shape GraphSelection should be null");
        assertTrue(step.getData().getFiles().isEmpty(), "Data files should be empty");
        assertNull(step.getData().getFileSelection(), "Data FileSelection should be null");
        assertEquals(1, step.getData().getGraphs().size(), "Should have one data graph");
        assertEquals("data:graph", step.getData().getGraphs().get(0), "Data graph should match");
        assertNull(step.getInferred().getGraph(), "Inferred graph should be null");
        assertEquals(
                "target/inferred.ttl", step.getInferred().getFile(), "Inferred file should match");
        assertFalse(step.isIterateUntilStable(), "iterateUntilStable should be false");
    }

    @Test
    void testParseShaclInferStepEmptyConfig() throws Exception {
        String xml = """
                <shaclInfer/>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty config");
    }

    @Test
    void testParseShaclInferStepMissingShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for missing shapes");
    }

    @Test
    void testParseShaclInferStepEmptyShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes/>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);
        assertEquals(List.of("data.ttl"), step.getData().getFiles());
        assertEquals("inferred:graph", step.getInferred().getGraph());
    }

    @Test
    void testParseShaclInferStepMissingData() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for missing data");
    }

    @Test
    void testParseShaclInferStepMissingInferred() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for missing inferred");
    }

    @Test
    void testParseShaclInferStepEmptyFileInShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file></file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty file in shapes");
    }

    @Test
    void testParseShaclInferStepEmptyFilesInShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <files/>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty files in shapes");
    }

    @Test
    void testParseShaclInferStepEmptyGraphInShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <graph></graph>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty graph in shapes");
    }

    @Test
    void testParseShaclInferStepEmptyGraphsInShapes() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <graphs/>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty graphs in shapes");
    }

    @Test
    void testParseShaclInferStepEmptyData() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data/>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);
        assertEquals(List.of("shapes.ttl"), step.getShapes().getFiles());
        assertEquals("inferred:graph", step.getInferred().getGraph());
    }

    @Test
    void testParseShaclInferStepEmptyInferred() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred/>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseShaclInferStep(config),
                "Should throw for empty inferred element");
    }

    @Test
    void testParseShaclInferStepWhitespaceValues() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <message>  Inferring triples  </message>
                    <shapes>
                        <file>  shapes.ttl  </file>
                    </shapes>
                    <data>
                        <file>  data.ttl  </file>
                    </data>
                    <inferred>
                        <graph>  inferred:graph  </graph>
                        <file>  target/inferred.ttl  </file>
                    </inferred>
                    <iterateUntilStable>  true  </iterateUntilStable>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);

        assertEquals("Inferring triples", step.getMessage(), "Message should be trimmed");
        assertEquals(1, step.getShapes().getFiles().size(), "Should have one shape file");
        assertEquals(
                "shapes.ttl", step.getShapes().getFiles().get(0), "Shape file should be trimmed");
        assertEquals(1, step.getData().getFiles().size(), "Should have one data file");
        assertEquals("data.ttl", step.getData().getFiles().get(0), "Data file should be trimmed");
        assertEquals(
                "inferred:graph",
                step.getInferred().getGraph(),
                "Inferred graph should be trimmed");
        assertEquals(
                "target/inferred.ttl",
                step.getInferred().getFile(),
                "Inferred file should be trimmed");
        assertTrue(step.isIterateUntilStable(), "iterateUntilStable should be true");
    }

    @Test
    void testParseShaclInferStepInvalidIterateUntilStable() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                    <iterateUntilStable>invalid</iterateUntilStable>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclInferStep step = parseShaclInferStep(config);

        // Boolean.parseBoolean("invalid") returns false
        assertFalse(
                step.isIterateUntilStable(), "Invalid iterateUntilStable should default to false");
    }

    @Test
    void testParseShaclInferStepEmptyMessage() throws Exception {
        String xml =
                """
                <shaclInfer>
                    <message></message>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <inferred>
                        <graph>inferred:graph</graph>
                    </inferred>
                </shaclInfer>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(ConfigurationParseException.class, () -> parseShaclInferStep(config));
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }

    private ShaclInferStep parseShaclInferStep(Xpp3Dom config) throws Exception {
        Pipeline pipeline = loadPipelineConfig(config);
        return (ShaclInferStep) pipeline.getSteps().get(0);
    }

    private Pipeline loadPipelineConfig(Xpp3Dom config) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(new File("."));
        List<Step> steps = new ArrayList<>();
        steps.add(ShaclInferStep.parse(config));
        pipeline.setSteps(steps);
        return pipeline;
    }
}
