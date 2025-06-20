package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.pipeline.step.*;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class ForeachStepParseTests {

    @Test
    void testParseValidForeachStepWithSparqlUpdate() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                            <exclude>vocab:temp</exclude>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;http://example.org/s&gt; &lt;http://example.org/p&gt; &lt;http://example.org/o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        ForeachStep step = parseForeachStep(config);

        assertEquals("fileGraph", step.getVar(), "Variable name should match");
        assertNotNull(step.getValues(), "Values should not be null");
        assertEquals(
                1,
                step.getValues().getGraphs().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "vocab:*",
                step.getValues().getGraphs().getInclude().get(0),
                "Include pattern should match");
        assertEquals(
                1,
                step.getValues().getGraphs().getExclude().size(),
                "Should have one exclude pattern");
        assertEquals(
                "vocab:temp",
                step.getValues().getGraphs().getExclude().get(0),
                "Exclude pattern should match");
        assertEquals(1, step.getBody().size(), "Should have one body step");
        assertTrue(
                step.getBody().get(0) instanceof SparqlUpdateStep,
                "Body step should be SparqlUpdateStep");
        SparqlUpdateStep bodyStep = (SparqlUpdateStep) step.getBody().get(0);
        assertEquals(
                "INSERT DATA { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }",
                bodyStep.getSparql().trim(),
                "SPARQL query should match");
    }

    @Test
    void testParseValidForeachStepWithMultipleBodySteps() throws Exception {
        String xml =
                """
                <foreach>
                    <var>graph</var>
                    <values>
                        <graphs>
                            <include>test:*</include>
                        </graphs>
                    </values>
                    <body>
                        <add>
                            <graph>source:graph</graph>
                            <toGraph>?graph</toGraph>
                        </add>
                        <write>
                            <graph>?graph</graph>
                            <toFile>target/output.ttl</toFile>
                        </write>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        ForeachStep step = parseForeachStep(config);

        assertEquals("graph", step.getVar(), "Variable name should match");
        assertNotNull(step.getValues(), "Values should not be null");
        assertEquals(
                1,
                step.getValues().getGraphs().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "test:*",
                step.getValues().getGraphs().getInclude().get(0),
                "Include pattern should match");
        assertTrue(step.getValues().getGraphs().getExclude().isEmpty(), "Excludes should be empty");
        assertEquals(2, step.getBody().size(), "Should have two body steps");
        assertTrue(step.getBody().get(0) instanceof AddStep, "First body step should be AddStep");
        assertTrue(
                step.getBody().get(1) instanceof WriteStep, "Second body step should be WriteStep");
        AddStep addStep = (AddStep) step.getBody().get(0);
        assertEquals(
                "source:graph",
                addStep.getInputsComponent().getGraphs().get(0),
                "AddStep graph should match");
        assertEquals("?graph", addStep.getToGraph(), "AddStep toGraph should match");
        WriteStep writeStep = (WriteStep) step.getBody().get(1);
        assertEquals("?graph", writeStep.getGraphs().get(0), "WriteStep graph should match");
        assertEquals("target/output.ttl", writeStep.getToFile(), "WriteStep toFile should match");
    }

    @Test
    void testParseEmptyForeachStep() throws Exception {
        String xml = """
                <foreach/>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for empty config");
    }

    @Test
    void testParseMissingVar() throws Exception {
        String xml =
                """
                <foreach>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for missing var");
    }

    @Test
    void testParseEmptyVar() throws Exception {
        String xml =
                """
                <foreach>
                    <var></var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for empty var");
    }

    @Test
    void testParseWhitespaceVar() throws Exception {
        String xml =
                """
                <foreach>
                    <var>  </var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for whitespace-only var");
    }

    @Test
    void testParseMissingValues() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for missing values");
    }

    @Test
    void testParseEmptyValues() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values/>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for empty values");
    }

    @Test
    void testParseMissingBody() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for missing body");
    }

    @Test
    void testParseEmptyBody() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body/>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for empty body");
    }

    @Test
    void testParseInvalidBodyStepType() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body>
                        <invalidStep>
                            <param>value</param>
                        </invalidStep>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> parseForeachStep(config),
                "Should throw for invalid step type in body");
    }

    @Test
    void testParseWhitespaceValues() throws Exception {
        String xml =
                """
                <foreach>
                    <var>  fileGraph  </var>
                    <values>
                        <graphs>
                            <include>  vocab:*  </include>
                            <exclude>  vocab:temp  </exclude>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>  INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }  </sparql>
                        </sparqlUpdate>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        ForeachStep step = parseForeachStep(config);

        assertEquals("fileGraph", step.getVar(), "Variable name should be trimmed");
        assertNotNull(step.getValues(), "Values should not be null");
        assertEquals(
                1,
                step.getValues().getGraphs().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "vocab:*",
                step.getValues().getGraphs().getInclude().get(0),
                "Include pattern should be trimmed");
        assertEquals(
                1,
                step.getValues().getGraphs().getExclude().size(),
                "Should have one exclude pattern");
        assertEquals(
                "vocab:temp",
                step.getValues().getGraphs().getExclude().get(0),
                "Exclude pattern should be trimmed");
        assertEquals(1, step.getBody().size(), "Should have one body step");
        assertTrue(
                step.getBody().get(0) instanceof SparqlUpdateStep,
                "Body step should be SparqlUpdateStep");
        SparqlUpdateStep bodyStep = (SparqlUpdateStep) step.getBody().get(0);
        assertEquals(
                "INSERT DATA { GRAPH ?fileGraph { <s> <p> <o> } }",
                bodyStep.getSparql().trim(),
                "SPARQL query should be trimmed");
    }

    @Test
    void testParseNestedForeachStep() throws Exception {
        String xml =
                """
                <foreach>
                    <var>outerGraph</var>
                    <values>
                        <graphs>
                            <include>outer:*</include>
                        </graphs>
                    </values>
                    <body>
                        <foreach>
                            <var>innerGraph</var>
                            <values>
                                <graphs>
                                    <include>inner:*</include>
                                </graphs>
                            </values>
                            <body>
                                <sparqlUpdate>
                                    <sparql>INSERT DATA { GRAPH ?innerGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                                </sparqlUpdate>
                            </body>
                        </foreach>
                    </body>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        ForeachStep step = parseForeachStep(config);

        assertEquals("outerGraph", step.getVar(), "Outer variable name should match");
        assertNotNull(step.getValues(), "Outer values should not be null");
        assertEquals(
                1,
                step.getValues().getGraphs().getInclude().size(),
                "Outer should have one include pattern");
        assertEquals(
                "outer:*",
                step.getValues().getGraphs().getInclude().get(0),
                "Outer include pattern should match");
        assertEquals(1, step.getBody().size(), "Outer should have one body step");
        assertTrue(step.getBody().get(0) instanceof ForeachStep, "Body step should be ForeachStep");
        ForeachStep innerStep = (ForeachStep) step.getBody().get(0);
        assertEquals("innerGraph", innerStep.getVar(), "Inner variable name should match");
        assertNotNull(innerStep.getValues(), "Inner values should not be null");
        assertEquals(
                1,
                innerStep.getValues().getGraphs().getInclude().size(),
                "Inner should have one include pattern");
        assertEquals(
                "inner:*",
                innerStep.getValues().getGraphs().getInclude().get(0),
                "Inner include pattern should match");
        assertEquals(1, innerStep.getBody().size(), "Inner should have one body step");
        assertTrue(
                innerStep.getBody().get(0) instanceof SparqlUpdateStep,
                "Inner body step should be SparqlUpdateStep");
    }

    @Test
    void testParseNullConfig() throws Exception {
        assertThrows(
                ConfigurationParseException.class,
                () -> ForeachStep.parse(null),
                "Should throw for null config");
    }

    @Test
    void testParseUnexpectedChildElement() throws Exception {
        String xml =
                """
                <foreach>
                    <var>fileGraph</var>
                    <values>
                        <graphs>
                            <include>vocab:*</include>
                        </graphs>
                    </values>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { GRAPH ?fileGraph { &lt;s&gt; &lt;p&gt; &lt;o&gt; } }</sparql>
                        </sparqlUpdate>
                    </body>
                    <unexpected>value</unexpected>
                </foreach>
                """;
        Xpp3Dom config = buildConfig(xml);
        ForeachStep step = parseForeachStep(config);

        assertEquals("fileGraph", step.getVar(), "Variable name should match");
        assertNotNull(step.getValues(), "Values should not be null");
        assertEquals(
                1,
                step.getValues().getGraphs().getInclude().size(),
                "Should have one include pattern");
        assertEquals(
                "vocab:*",
                step.getValues().getGraphs().getInclude().get(0),
                "Include pattern should match");
        assertEquals(1, step.getBody().size(), "Should have one body step");
        // ParsingHelper ignores unexpected elements, so parsing succeeds
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }

    private ForeachStep parseForeachStep(Xpp3Dom config) throws Exception {
        Pipeline pipeline = loadPipelineConfig(config);
        return (ForeachStep) pipeline.getSteps().get(0);
    }

    private Pipeline loadPipelineConfig(Xpp3Dom config) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId("test-pipeline");
        pipeline.setMetadataGraph(RDFIO.metadataGraph.toString());
        pipeline.setBaseDir(new File("."));
        List<Step> steps = new ArrayList<>();
        steps.add(ForeachStep.parse(config));
        pipeline.setSteps(steps);
        return pipeline;
    }
}
