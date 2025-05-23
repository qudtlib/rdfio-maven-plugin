package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.WriteStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class WriteStepParseTests {

    @Test
    void testParseBasic() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        assertEquals(List.of("test:graph"), step.getGraphs(), "Graph should match");
        assertNull(step.getToFile(), "toFile should be null");
    }

    @Test
    void testParseWithToFile() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        assertEquals(List.of("test:graph"), step.getGraphs(), "Graph should match");
        assertEquals("target/output.ttl", step.getToFile(), "toFile should match");
    }

    @Test
    void testParseEmptyConfig() throws Exception {
        String xml = """
                <write/>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> WriteStep.parse(config),
                "Should throw for missing graph");
    }

    @Test
    void testParseMissingGraph() throws Exception {
        String xml =
                """
                <write>
                    <toFile>target/output.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);
        assertEquals("target/output.ttl", step.getToFile());
    }

    @Test
    void testParseEmptyGraph() throws Exception {
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
                "Should throw for empty graph");
    }

    @Test
    void testParseWhitespaceGraph() throws Exception {
        String xml =
                """
                <write>
                    <graph>  </graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> WriteStep.parse(config),
                "Should throw for whitespace-only graph");
    }

    @Test
    void testParseEmptyToFile() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile></toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(ConfigurationParseException.class, () -> WriteStep.parse(config));
    }

    @Test
    void testParseWhitespaceToFile() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>  </toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(ConfigurationParseException.class, () -> WriteStep.parse(config));
    }

    @Test
    void testParseWhitespaceValues() throws Exception {
        String xml =
                """
                <write>
                    <graph>  test:graph  </graph>
                    <toFile>  target/output.ttl  </toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);
        assertEquals(
                List.of("test:graph"), step.getGraphs(), "Graph should have no whitespace around");
        assertEquals(
                "target/output.ttl", step.getToFile(), "toFile should have no whitespace around");
    }

    @Test
    void testParseNullConfig() throws Exception {
        assertThrows(
                ConfigurationParseException.class,
                () -> WriteStep.parse(null),
                "Should throw for null config");
    }

    @Test
    void testParseMultipleGraphElements() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph1</graph>
                    <graph>test:graph2</graph>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);

        WriteStep step = WriteStep.parse(config);
        assertEquals(
                List.of("test:graph1", "test:graph2"), step.getGraphs(), "Graphs should match");
    }

    @Test
    void testParseMultipleToFileElements() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <toFile>target/output1.ttl</toFile>
                    <toFile>target/output2.ttl</toFile>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);

        assertThrows(ConfigurationParseException.class, () -> WriteStep.parse(config));
    }

    @Test
    void testParseUnexpectedChildElement() throws Exception {
        String xml =
                """
                <write>
                    <graph>test:graph</graph>
                    <unexpected>value</unexpected>
                </write>
                """;
        Xpp3Dom config = buildConfig(xml);
        WriteStep step = WriteStep.parse(config);

        assertEquals(List.of("test:graph"), step.getGraphs(), "Graph should match");
        assertNull(step.getToFile(), "toFile should be null");
        // ParseHelper ignores unexpected elements, so no exception is thrown
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }
}
