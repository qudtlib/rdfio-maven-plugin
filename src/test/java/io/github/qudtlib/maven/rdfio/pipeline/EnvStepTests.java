package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.EnvStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QuerySolutionMap;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EnvStepTests {

    private Dataset dataset;
    private PipelineState state;

    @BeforeEach
    void setUp() throws Exception {
        dataset = DatasetFactory.create();
        File baseDir = new File(".");
        RelativePath workBaseDir = new RelativePath(baseDir, "target");
        state =
                new PipelineState(
                        "test-pipeline",
                        baseDir,
                        workBaseDir.subDir("rdfio").subDir("pipelines"),
                        new StdoutLog(),
                        null,
                        null);
        state.files().mkdirs(workBaseDir);
    }

    private static Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    }

    @Test
    void testSetsSingleVariable() throws Exception {
        String xml =
                """
                <env>
                    <property name="suffix">-ext</property>
                </env>
                """;
        EnvStep.parse(buildConfig(xml)).execute(dataset, state);

        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertEquals("-ext", bindings.get("suffix").asLiteral().getString());
    }

    @Test
    void testSetsMultipleVariables() throws Exception {
        String xml =
                """
                <env>
                    <property name="alpha">A</property>
                    <property name="beta">B</property>
                </env>
                """;
        EnvStep.parse(buildConfig(xml)).execute(dataset, state);

        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertEquals("A", bindings.get("alpha").asLiteral().getString());
        assertEquals("B", bindings.get("beta").asLiteral().getString());
    }

    @Test
    void testEmptyValueAllowed() throws Exception {
        String xml =
                """
                <env>
                    <property name="suffix"></property>
                </env>
                """;
        EnvStep.parse(buildConfig(xml)).execute(dataset, state);

        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertEquals("", bindings.get("suffix").asLiteral().getString());
    }

    @Test
    void testOverridesExistingVariable() throws Exception {
        String xmlFirst =
                """
                <env><property name="x">old</property></env>
                """;
        String xmlSecond =
                """
                <env><property name="x">new</property></env>
                """;
        EnvStep.parse(buildConfig(xmlFirst)).execute(dataset, state);
        EnvStep.parse(buildConfig(xmlSecond)).execute(dataset, state);

        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertEquals("new", bindings.get("x").asLiteral().getString());
    }

    @Test
    void testMissingPropertyNameThrows() {
        String xml =
                """
                <env>
                    <property>value</property>
                </env>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> EnvStep.parse(buildConfig(xml)),
                "Missing name attribute should throw");
    }

    @Test
    void testUnknownChildThrows() {
        String xml =
                """
                <env>
                    <unknown>foo</unknown>
                </env>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> EnvStep.parse(buildConfig(xml)),
                "Unknown child element should throw");
    }
}
