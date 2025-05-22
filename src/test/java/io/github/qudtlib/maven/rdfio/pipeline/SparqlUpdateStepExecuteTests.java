package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.qudtlib.maven.rdfio.common.RDFIO;
import io.github.qudtlib.maven.rdfio.common.file.RdfFileProcessor;
import io.github.qudtlib.maven.rdfio.common.file.RelativePath;
import io.github.qudtlib.maven.rdfio.common.sparql.SparqlHelper;
import io.github.qudtlib.maven.rdfio.pipeline.step.SparqlUpdateStep;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparqlUpdateStepExecuteTests {
    private Dataset dataset;
    private PipelineState state;
    private RelativePath testOutputBase;
    private String pipelineId;

    @BeforeEach
    void setUp() {
        dataset = DatasetFactory.create();
        File baseDir = new File(".");
        baseDir.mkdirs();
        RelativePath workBaseDir = new RelativePath(baseDir, "target");
        pipelineId = "test-pipeline";
        state =
                new PipelineState(
                        pipelineId,
                        baseDir,
                        workBaseDir.subDir("rdfio").subDir("pipelines"),
                        null,
                        null,
                        null);
        testOutputBase = workBaseDir.subDir("test-output");
        state.files().mkdirs(workBaseDir);
        state.files().mkdirs(testOutputBase);
    }

    @Test
    void testExecuteWithValidInlineSparql() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>
                        <![CDATA[
                            INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }
                        ]]>
                    </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should contain the inserted triple");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithValidFileSparql() throws Exception {
        String sparqlContent =
                "INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }";
        RelativePath sparqlFile = testOutputBase.subFile("update.rq");
        state.files().writeText(sparqlFile, sparqlContent);
        String xml =
                """
                <sparqlUpdate>
                    <file>target/test-output/update.rq</file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should contain the inserted triple");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithVariableBindings() throws Exception {
        Model metaModel = dataset.getNamedModel(state.getMetadataGraph());
        metaModel.add(
                RDFIO.makeVariableUri("fileGraph"),
                RDFIO.value,
                ResourceFactory.createResource("test:graph"));

        String xml =
                """
                <sparqlUpdate>
                    <sparql>
                        <![CDATA[
                            INSERT { GRAPH ?fileGraph { <http://example.org/s> <http://example.org/p> <http://example.org/o> } } WHERE {}
                        ]]>
                    </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should contain the inserted triple using variable binding");
    }

    @Test
    void testExecuteWithMissingSparqlAndFile() throws Exception {
        SparqlUpdateStep step = new SparqlUpdateStep();

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when both sparql and file are missing");
    }

    @Test
    void testExecuteWithNonExistentFile() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <file>target/test-output/nonexistent.rq</file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when SPARQL file does not exist");
    }

    @Test
    void testExecuteWithMalformedSparql() throws Exception {
        String xml =
                """
                <sparqlUpdate>
                    <sparql>INVALID SPARQL</sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when SPARQL query is malformed");
    }

    @Test
    void testExecuteWithEmptyFile() throws Exception {
        RelativePath sparqlFile = testOutputBase.subFile("empty.rq");
        state.files().writeText(sparqlFile, "");
        String xml =
                """
                <sparqlUpdate>
                    <file>target/test-output/empty.rq</file>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        assertThrows(
                MojoExecutionException.class,
                () -> step.execute(dataset, state),
                "Should throw when SPARQL file is empty");
    }

    @Test
    void testExecuteWithExistingData() throws Exception {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/existing"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        String xml =
                """
                <sparqlUpdate>

                    <sparql><![CDATA[ INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }]]></sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/existing"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should retain existing triple");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createResource("http://example.org/o")),
                "Target graph should contain the inserted triple");
    }

    @Test
    void testExecuteWithDeleteOperation() throws Exception {
        Model model = dataset.getNamedModel("test:graph");
        model.add(
                ResourceFactory.createResource("http://example.org/s"),
                ResourceFactory.createProperty("http://example.org/p"),
                ResourceFactory.createResource("http://example.org/o"));

        String xml =
                """
                <sparqlUpdate>
                    <sparql>
                        <![CDATA[
                            DELETE WHERE { GRAPH <test:graph> { ?s ?p ?o } }
                        ]]>
                    </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        assertTrue(model.isEmpty(), "Target graph should be empty after deletion");
        assertTrue(
                state.getPrecedingSteps().contains(step),
                "Step should be added to preceding steps");
    }

    @Test
    void testExecuteWithShaclFunctions() throws Exception {
        // Setup SHACL functions
        File shaclFile = new File("src/test/resources/shaclFunction.ttl");
        assertTrue(shaclFile.exists(), "SHACL function file must exist");
        Model shaclModel = dataset.getNamedModel(state.getShaclFunctionsGraph());
        RdfFileProcessor.loadRdfFiles(List.of(shaclFile), shaclModel);
        SparqlHelper.registerShaclFunctions(
                dataset, state.getShaclFunctionsGraph(), state.getLog());

        String xml =
                """
                <sparqlUpdate>
                    <sparql>
                        <![CDATA[
                        INSERT {
                            GRAPH <test:graph> {
                                <http://example.org/s> <http://example.org/p> ?funcResult .
                            }
                        } WHERE {
                            BIND(ex:myFunction("testCall") AS ?funcResult)
                        }
                        ]]>
                    </sparql>
                </sparqlUpdate>
                """;
        Xpp3Dom config = buildConfig(xml);
        SparqlUpdateStep step = SparqlUpdateStep.parse(config);

        step.execute(dataset, state);

        Model model = dataset.getNamedModel("test:graph");
        assertTrue(
                model.contains(
                        ResourceFactory.createResource("http://example.org/s"),
                        ResourceFactory.createProperty("http://example.org/p"),
                        ResourceFactory.createStringLiteral("Processed: testCall")),
                "Target graph should contain the inserted triple with SHACL function");
    }

    private Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8.name());
    }
}
