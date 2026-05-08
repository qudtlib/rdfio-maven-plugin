package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.ForEachEnvStep;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.ResourceFactory;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ForEachEnvStepExecuteTests {

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

    private static ForEachEnvStep parseStep(String xml) throws Exception {
        return ForEachEnvStep.parse(buildConfig(xml));
    }

    @Test
    void testBodyExecutesOncePerEnv() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="alpha"><property name="suffix">-alpha</property></env>
                    <env id="beta"><property name="suffix">-beta</property></env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("result-alpha:graph"), "alpha graph should exist");
        assertTrue(dataset.containsNamedModel("result-beta:graph"), "beta graph should exist");
    }

    @Test
    void testVariablesIsolatedBetweenIterations() throws Exception {
        // After each iteration the env variables should be cleared so the next
        // iteration starts clean. A variable set in env A must not bleed into env B.
        String xml =
                """
                <forEachEnv>
                    <env id="first">
                        <property name="suffix">-first</property>
                        <property name="extra">only-in-first</property>
                    </env>
                    <env id="second">
                        <property name="suffix">-second</property>
                    </env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("result-first:graph"));
        assertTrue(dataset.containsNamedModel("result-second:graph"));
        // After the loop, "extra" and "suffix" must be gone
        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertNull(bindings.get("suffix"), "suffix should be cleared after loop");
        assertNull(bindings.get("extra"), "extra should be cleared after loop");
    }

    @Test
    void testPreExistingVariablesRestoredAfterLoop() throws Exception {
        // A variable set before the loop must be restored after the loop finishes.
        PipelineHelper.setPipelineVariable(
                dataset, state, "preExisting", ResourceFactory.createStringLiteral("preserved"));

        String xml =
                """
                <forEachEnv>
                    <env id="only">
                        <property name="preExisting">overridden</property>
                    </env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <g:x> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;

        parseStep(xml).execute(dataset, state);

        QuerySolutionMap bindings =
                SparqlHelper.extractVariableBindings(dataset, state.getMetadataGraph());
        assertEquals(
                "preserved",
                bindings.get("preExisting").asLiteral().getString(),
                "pre-loop variable must be restored");
    }

    @Test
    void testEmptyPropertyValue() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="core"><property name="suffix"></property></env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(
                dataset.containsNamedModel("result:graph"),
                "Empty suffix should produce bare graph name");
    }

    @Test
    void testStepAddedToPrecedingSteps() throws Exception {
        String xml =
                """
                <forEachEnv>
                    <env id="x"><property name="g">g:x</property></env>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <${g}> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachEnv>
                """;
        ForEachEnvStep step = parseStep(xml);
        step.execute(dataset, state);
        assertTrue(state.getPrecedingSteps().contains(step), "Step should be in precedingSteps");
    }
}
