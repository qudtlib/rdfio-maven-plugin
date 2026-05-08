package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.InvokeStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.SparqlUpdateStep;
import io.github.qudtlib.maven.rdfio.pipeline.step.StepDefStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class StepDefInvokeParseTests {

    private static Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    }

    // --- stepDef ---

    @Test
    void testParseValidStepDef() throws Exception {
        String xml =
                """
                <stepDef id="my-def">
                    <sparqlUpdate>
                        <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                    </sparqlUpdate>
                </stepDef>
                """;
        Xpp3Dom config = buildConfig(xml);
        StepDefStep step = StepDefStep.parse(config);

        assertEquals("my-def", step.getId());
        assertEquals(1, step.getSteps().size());
        assertInstanceOf(SparqlUpdateStep.class, step.getSteps().get(0));
    }

    @Test
    void testParseStepDefWithMultipleSteps() throws Exception {
        String xml =
                """
                <stepDef id="two-steps">
                    <sparqlUpdate>
                        <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                    </sparqlUpdate>
                    <sparqlUpdate>
                        <sparql>DELETE WHERE { &lt;urn:s&gt; ?p ?o }</sparql>
                    </sparqlUpdate>
                </stepDef>
                """;
        StepDefStep step = StepDefStep.parse(buildConfig(xml));
        assertEquals("two-steps", step.getId());
        assertEquals(2, step.getSteps().size());
    }

    @Test
    void testParseStepDefMissingIdThrows() throws Exception {
        String xml =
                """
                <stepDef>
                    <sparqlUpdate>
                        <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                    </sparqlUpdate>
                </stepDef>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> StepDefStep.parse(buildConfig(xml)),
                "Missing id should throw");
    }

    @Test
    void testParseStepDefNoChildStepsThrows() throws Exception {
        String xml = "<stepDef id=\"empty\"></stepDef>";
        assertThrows(
                ConfigurationParseException.class,
                () -> StepDefStep.parse(buildConfig(xml)),
                "Empty stepDef should throw");
    }

    @Test
    void testParseStepDefNestedStepDefThrows() throws Exception {
        String xml =
                """
                <stepDef id="outer">
                    <stepDef id="inner">
                        <sparqlUpdate><sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql></sparqlUpdate>
                    </stepDef>
                </stepDef>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> StepDefStep.parse(buildConfig(xml)),
                "Nested stepDef should be rejected");
    }

    // --- invoke ---

    @Test
    void testParseValidInvoke() throws Exception {
        String xml = "<invoke stepRef=\"my-def\"/>";
        InvokeStep step = InvokeStep.parse(buildConfig(xml));
        assertEquals("my-def", step.getStepRef());
    }

    @Test
    void testParseInvokeMissingStepRefThrows() throws Exception {
        String xml = "<invoke/>";
        assertThrows(
                ConfigurationParseException.class,
                () -> InvokeStep.parse(buildConfig(xml)),
                "Missing stepRef should throw");
    }

    @Test
    void testParseInvokeStepRefWhitespaceThrows() throws Exception {
        String xml = "<invoke stepRef=\"   \"/>";
        assertThrows(
                ConfigurationParseException.class,
                () -> InvokeStep.parse(buildConfig(xml)),
                "Blank stepRef should throw");
    }
}
