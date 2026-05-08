package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.ForEachEnvStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class ForEachEnvStepParseTests {

    private static Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    }

    @Test
    void testParseValidForEachEnv() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="core">
                        <property name="suffix"></property>
                        <property name="dataGraph">dist:vocab:UNITS.ttl</property>
                    </env>
                    <env id="extensions">
                        <property name="suffix">-ext</property>
                        <property name="dataGraph">dist:ext:vocab</property>
                    </env>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;

        ForEachEnvStep step = ForEachEnvStep.parse(buildConfig(xml));

        assertEquals(2, step.getEnvs().size());
        assertEquals("", step.getEnvs().get(0).getProperties().get("suffix"));
        assertEquals(
                "dist:vocab:UNITS.ttl", step.getEnvs().get(0).getProperties().get("dataGraph"));
        assertEquals("-ext", step.getEnvs().get(1).getProperties().get("suffix"));
        assertEquals(1, step.getBody().size());
    }

    @Test
    void testParseOptionalMessage() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <message>Processing envs</message>
                    <env id="only">
                        <property name="x">val</property>
                    </env>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;
        ForEachEnvStep step = ForEachEnvStep.parse(buildConfig(xml));
        assertEquals("Processing envs", step.getMessage());
    }

    @Test
    void testMissingBodyThrows() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="core"><property name="x">y</property></env>
                </forEachEnv>
                """;
        assertThrows(
                ConfigurationParseException.class, () -> ForEachEnvStep.parse(buildConfig(xml)));
    }

    @Test
    void testMissingEnvThrows() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;
        assertThrows(
                ConfigurationParseException.class, () -> ForEachEnvStep.parse(buildConfig(xml)));
    }

    @Test
    void testUnknownTopLevelChildThrows() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="core"><property name="x">y</property></env>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                    <unknown>foo</unknown>
                </forEachEnv>
                """;
        assertThrows(
                ConfigurationParseException.class, () -> ForEachEnvStep.parse(buildConfig(xml)));
    }

    @Test
    void testSavepointInBodyThrows() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="core"><property name="x">y</property></env>
                    <body>
                        <savepoint><id>sp</id></savepoint>
                    </body>
                </forEachEnv>
                """;
        assertThrows(
                ConfigurationParseException.class, () -> ForEachEnvStep.parse(buildConfig(xml)));
    }
}
