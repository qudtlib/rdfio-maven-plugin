package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.pipeline.step.SparqlUpdateStep;
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

public class SparqlUpdateStepParseTests {

    @Test
    void testParseSparqlUpdateStepWithInlineSparql() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>INSERT DATA { GRAPH &lt;test:graph&gt; { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }</sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = parseSparqlUpdateStep(config);

        assertEquals(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }",
                step.getSparql().trim(),
                "Sparql query should match");
        assertNull(step.getFile(), "File should be null");
    }

    @Test
    void testParseSparqlUpdateStepWithFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <file>src/main/resources/update.rq</file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = parseSparqlUpdateStep(config);

        assertNull(step.getSparql(), "Sparql should be null");
        assertEquals("src/main/resources/update.rq", step.getFile(), "File path should match");
    }

    @Test
    void testParseSparqlUpdateStepWithCDATA() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql><![CDATA[INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }]]></sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = parseSparqlUpdateStep(config);

        assertEquals(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }",
                step.getSparql().trim(),
                "Sparql query should match");
        assertNull(step.getFile(), "File should be null");
    }

    @Test
    void testParseSparqlUpdateStepEmptyConfig() throws Exception {
        String xml = """
                <sparqlUpdate/>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw for empty config");
    }

    @Test
    void testParseSparqlUpdateStepMissingSparqlAndFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <other>something</other>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw when both sparql and file are missing");
    }

    @Test
    void testParseSparqlUpdateStepBothSparqlAndFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>INSERT DATA { GRAPH &lt;test:graph&gt; { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }</sparql>
                    <file>src/main/resources/update.rq</file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw when both sparql and file are provided");
    }

    @Test
    void testParseSparqlUpdateStepEmptySparql() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql></sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw for empty sparql");
    }

    @Test
    void testParseSparqlUpdateStepWhitespaceSparql() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>  </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw for whitespace-only sparql");
    }

    @Test
    void testParseSparqlUpdateStepEmptyFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <file></file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw for empty file");
    }

    @Test
    void testParseSparqlUpdateStepWhitespaceFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <file>  </file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseSparqlUpdateStep(config),
                "Should throw for whitespace-only file");
    }

    @Test
    void testParseSparqlUpdateStepTrimmedValues() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>  INSERT DATA { GRAPH &lt;test:graph&gt; { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }  </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = parseSparqlUpdateStep(config);

        assertEquals(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }",
                step.getSparql().trim(),
                "Sparql query should be trimmed");
        assertNull(step.getFile(), "File should be null");
    }

    @Test
    void testParseSparqlUpdateStepWithUnexpectedChild() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>INSERT DATA { GRAPH &lt;test:graph&gt; { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }</sparql>
                    <unexpected>value</unexpected>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = parseSparqlUpdateStep(config);

        assertEquals(
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }",
                step.getSparql().trim(),
                "Sparql query should match");
        assertNull(step.getFile(), "File should be null");
        // ParsingHelper ignores unexpected elements
    }

    @Test
    void testParseSparqlUpdateStepNullConfig() throws Exception {
        assertThrows(
                ConfigurationParseException.class,
                () -> SparqlUpdateStep.parse(null),
                "Should throw for null config");
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }

    private SparqlUpdateStep parseSparqlUpdateStep(Xpp3Dom config) throws Exception {
        Pipeline pipeline = loadPipelineConfig(config);
        return (SparqlUpdateStep) pipeline.getSteps().get(0);
    }

    private Pipeline loadPipelineConfig(Xpp3Dom config) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(new File("."));
        List<Step> steps = new ArrayList<>();
        steps.add(SparqlUpdateStep.parse(config));
        pipeline.setSteps(steps);
        return pipeline;
    }
}
