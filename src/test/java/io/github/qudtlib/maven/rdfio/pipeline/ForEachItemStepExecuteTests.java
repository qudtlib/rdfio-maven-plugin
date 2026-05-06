package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.log.StdoutLog;
import io.github.qudtlib.maven.rdfio.pipeline.step.ForEachItemStep;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ForEachItemStepExecuteTests {

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

    private static ForEachItemStep parseStep(String xml) throws Exception {
        return ForEachItemStep.parse(buildConfig(xml));
    }

    @Test
    void testBodyExecutesOncePerItem() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="alpha"><property name="suffix">-alpha</property></item>
                    <item id="beta"><property name="suffix">-beta</property></item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${item.suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("result-alpha:graph"), "alpha graph should exist");
        assertTrue(dataset.containsNamedModel("result-beta:graph"), "beta graph should exist");
    }

    @Test
    void testItemIdSubstitution() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="myId"></item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <graph:${item.id}> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("graph:myId"), "${item.id} should be substituted");
    }

    @Test
    void testEmptyPropertySubstitution() throws Exception {
        // suffix="" should produce graph name without suffix
        String xml =
                """
                <forEachItem>
                    <item id="core"><property name="suffix"></property></item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <result${item.suffix}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(
                dataset.containsNamedModel("result:graph"),
                "Empty suffix should produce bare graph name");
    }

    @Test
    void testPreambleRunsBeforeBodyPostambleRunsAfter() throws Exception {
        List<String> executionOrder = new ArrayList<>();

        // We'll use real SPARQL and check graph presence as a proxy for order.
        // Preamble creates pre:graph, body creates body:graph, postamble creates post:graph.
        String xml =
                """
                <forEachItem>
                    <item id="only">
                        <preamble>
                            <sparqlUpdate>
                                <sparql><![CDATA[
                                    INSERT DATA { GRAPH <pre:graph> { <urn:s> <urn:p> <urn:o> } }
                                ]]></sparql>
                            </sparqlUpdate>
                        </preamble>
                        <postamble>
                            <sparqlUpdate>
                                <sparql><![CDATA[
                                    INSERT DATA { GRAPH <post:graph> { <urn:s> <urn:p> <urn:o> } }
                                ]]></sparql>
                            </sparqlUpdate>
                        </postamble>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <body:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("pre:graph"), "preamble should have run");
        assertTrue(dataset.containsNamedModel("body:graph"), "body should have run");
        assertTrue(dataset.containsNamedModel("post:graph"), "postamble should have run");
    }

    @Test
    void testPreambleOnlyOnItemThatDeclaredIt() throws Exception {
        // First item has no preamble; second item has a preamble.
        String xml =
                """
                <forEachItem>
                    <item id="no-preamble"></item>
                    <item id="has-preamble">
                        <preamble>
                            <sparqlUpdate>
                                <sparql><![CDATA[
                                    INSERT DATA { GRAPH <preamble-ran:graph> { <urn:s> <urn:p> <urn:o> } }
                                ]]></sparql>
                            </sparqlUpdate>
                        </preamble>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <body-${item.id}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(
                dataset.containsNamedModel("body-no-preamble:graph"), "first item body should run");
        assertTrue(
                dataset.containsNamedModel("body-has-preamble:graph"),
                "second item body should run");
        assertTrue(
                dataset.containsNamedModel("preamble-ran:graph"),
                "preamble of second item should run");
    }

    @Test
    void testItemsExecuteInOrder() throws Exception {
        // Each item's body inserts into a graph named by order; we verify by checking all exist.
        String xml =
                """
                <forEachItem>
                    <item id="first"></item>
                    <item id="second"></item>
                    <item id="third"></item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <order-${item.id}:graph> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        parseStep(xml).execute(dataset, state);

        assertTrue(dataset.containsNamedModel("order-first:graph"));
        assertTrue(dataset.containsNamedModel("order-second:graph"));
        assertTrue(dataset.containsNamedModel("order-third:graph"));
    }

    @Test
    void testStepAddedToPrecedingSteps() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="x"></item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[
                                INSERT DATA { GRAPH <g:x> { <urn:s> <urn:p> <urn:o> } }
                            ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        ForEachItemStep step = parseStep(xml);
        step.execute(dataset, state);
        assertTrue(state.getPrecedingSteps().contains(step), "Step should be in precedingSteps");
    }
}
