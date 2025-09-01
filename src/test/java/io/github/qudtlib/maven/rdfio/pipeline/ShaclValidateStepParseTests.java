package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.ShaclValidateStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class ShaclValidateStepParseTests {

    @Test
    void testParseShaclValidateStepWithShapesAndData() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>src/main/resources/shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>src/main/resources/data.ttl</file>
                    </data>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclValidateStep step = ShaclValidateStep.parse(config);

        assertNotNull(step.getShapes(), "Shapes should not be null");
        assertEquals(1, step.getShapes().getFiles().size(), "Should have one shapes file");
        assertEquals(
                "src/main/resources/shapes.ttl",
                step.getShapes().getFiles().get(0),
                "Shapes file path should match");
        assertNotNull(step.getData(), "Data should not be null");
        assertEquals(1, step.getData().getFiles().size(), "Should have one data file");
        assertEquals(
                "src/main/resources/data.ttl",
                step.getData().getFiles().get(0),
                "Data file path should match");
    }

    @Test
    void testParseShaclValidateStepWithFailSeverity() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <failOnSeverity>Warning</failOnSeverity>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclValidateStep step = ShaclValidateStep.parse(config);
        assertEquals("Warning", step.getFailOnSeverity());
    }

    @Test
    void testParseShaclValidateStepWithFailForMissingInputGraphTrue() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <failForMissingInputGraph>true</failForMissingInputGraph>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclValidateStep step = ShaclValidateStep.parse(config);
        assertTrue(step.isFailForMissingInputGraph(), "failForMissingInputGraph should be true");
    }

    @Test
    void testParseShaclValidateStepWithFailForMissingInputGraphFalse() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                    <failForMissingInputGraph>false</failForMissingInputGraph>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclValidateStep step = ShaclValidateStep.parse(config);
        assertFalse(step.isFailForMissingInputGraph(), "failForMissingInputGraph should be false");
    }

    @Test
    void testParseShaclValidateStepWithFailForMissingInputGraphDefault() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                    <data>
                        <file>data.ttl</file>
                    </data>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        ShaclValidateStep step = ShaclValidateStep.parse(config);
        assertTrue(step.isFailForMissingInputGraph(), "failForMissingInputGraph should be true");
    }

    @Test
    void testParseShaclValidateStepMissingShapes() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <data>
                        <file>data.ttl</file>
                    </data>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> ShaclValidateStep.parse(config),
                "Should throw when shapes are missing");
    }

    @Test
    void testParseShaclValidateStepMissingData() throws Exception {
        String xml =
                """
                <shaclValidate>
                    <shapes>
                        <file>shapes.ttl</file>
                    </shapes>
                </shaclValidate>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> ShaclValidateStep.parse(config),
                "Should throw when data are missing");
    }

    @Test
    void testParseShaclValidateStepEmptyConfig() throws Exception {
        String xml = """
                <shaclValidate/>
                """;
        Xpp3Dom config = buildConfig(xml);
        assertThrows(
                ConfigurationParseException.class,
                () -> ShaclValidateStep.parse(config),
                "Should throw for empty config");
    }

    @Test
    void testParseShaclValidateStepNullConfig() throws Exception {
        assertThrows(
                ConfigurationParseException.class,
                () -> ShaclValidateStep.parse(null),
                "Should throw for null config");
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }
}
