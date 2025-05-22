package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.SavepointStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.IOException;
import java.io.StringReader;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Test;

public class SavepointStepParseTest {

    @Test
    public void testParseValidSavepointWithId()
            throws MojoExecutionException, XmlPullParserException, IOException {
        String xml =
                """
                <savepoint>
                    <id>sp001</id>
                </savepoint>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new StringReader(xml));
        SavepointStep step = SavepointStep.parse(config);

        assertNotNull(step, "Parsed SavepointStep should not be null");
        assertEquals("sp001", step.getId(), "Savepoint ID should match");
        assertTrue(step.isEnabled(), "Savepoint should be enabled by default");
    }

    @Test
    public void testParseValidSavepointWithIdAndEnabledFalse()
            throws MojoExecutionException, XmlPullParserException, IOException {
        String xml =
                """
                <savepoint>
                    <id>sp002</id>
                    <enabled>false</enabled>
                </savepoint>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new StringReader(xml));
        SavepointStep step = SavepointStep.parse(config);

        assertNotNull(step, "Parsed SavepointStep should not be null");
        assertEquals("sp002", step.getId(), "Savepoint ID should match");
        assertFalse(step.isEnabled(), "Savepoint enabled should be false");
    }

    @Test
    public void testParseValidSavepointWithIdAndEnabledTrue()
            throws MojoExecutionException, XmlPullParserException, IOException {
        String xml =
                """
                <savepoint>
                    <id>sp003</id>
                    <enabled>true</enabled>
                </savepoint>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new StringReader(xml));
        SavepointStep step = SavepointStep.parse(config);

        assertNotNull(step, "Parsed SavepointStep should not be null");
        assertEquals("sp003", step.getId(), "Savepoint ID should match");
        assertTrue(step.isEnabled(), "Savepoint enabled should be true");
    }

    @Test
    public void testParseNullConfig() {
        Exception exception =
                assertThrows(
                        ConfigurationParseException.class,
                        () -> SavepointStep.parse(null),
                        "Parsing null config should throw MojoExecutionException");

        assertTrue(
                exception.getMessage().contains("Savepoint step configuration is missing"),
                "Exception message should indicate missing configuration");
        assertTrue(
                exception.getMessage().contains("<savepoint>"),
                "Exception message should include usage example");
    }

    @Test
    public void testParseMissingId() throws XmlPullParserException, IOException {
        String xml =
                """
                <savepoint>
                    <enabled>true</enabled>
                </savepoint>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new StringReader(xml));

        Exception exception =
                assertThrows(
                        ConfigurationParseException.class,
                        () -> SavepointStep.parse(config),
                        "Parsing config without id should throw MojoExecutionException");

        assertTrue(
                exception.getMessage().contains("Savepoint step requires a non-empty <id>"),
                "Exception message should indicate missing ID");
        assertTrue(
                exception.getMessage().contains("<id>sp001</id>"),
                "Exception message should include usage example");
    }

    @Test
    public void testParseEmptyId() throws XmlPullParserException, IOException {
        String xml =
                """
                <savepoint>
                    <id>   </id>
                </savepoint>
                """;
        Xpp3Dom config = Xpp3DomBuilder.build(new StringReader(xml));

        Exception exception =
                assertThrows(
                        ConfigurationParseException.class,
                        () -> SavepointStep.parse(config),
                        "Parsing config with empty id should throw MojoExecutionException");

        assertTrue(
                exception.getMessage().contains("Savepoint step requires a non-empty <id>"),
                "Exception message should indicate empty ID");
    }

    private static Xpp3Dom Xpp3DomBuilder_build(String xml) {
        try {
            return Xpp3DomBuilder.build(new java.io.StringReader(xml));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse XML: " + e.getMessage(), e);
        }
    }
}
