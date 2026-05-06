package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.qudtlib.maven.rdfio.pipeline.step.ForEachItemStep;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.Test;

public class ForEachItemStepParseTests {

    private static Xpp3Dom buildConfig(String xml) throws Exception {
        return Xpp3DomBuilder.build(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    }

    @Test
    void testParseValidForEach() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="core">
                        <property name="suffix"></property>
                        <property name="dataGraphs">dist:vocab:UNITS.ttl</property>
                    </item>
                    <item id="extensions">
                        <property name="suffix">-ext</property>
                        <property name="dataGraphs">dist:ext:vocab</property>
                        <preamble>
                            <sparqlUpdate>
                                <sparql>INSERT DATA { &lt;urn:pre&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                            </sparqlUpdate>
                        </preamble>
                        <postamble>
                            <sparqlUpdate>
                                <sparql>INSERT DATA { &lt;urn:post&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                            </sparqlUpdate>
                        </postamble>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;

        ForEachItemStep step = ForEachItemStep.parse(buildConfig(xml));

        assertEquals(2, step.getItems().size());

        ForEachItemStep.Item core = step.getItems().get(0);
        assertEquals("core", core.getId());
        assertEquals("", core.getProperties().get("suffix"));
        assertEquals("dist:vocab:UNITS.ttl", core.getProperties().get("dataGraphs"));
        assertNull(core.getPreambleDom());
        assertNull(core.getPostambleDom());

        ForEachItemStep.Item ext = step.getItems().get(1);
        assertEquals("extensions", ext.getId());
        assertEquals("-ext", ext.getProperties().get("suffix"));
        assertNotNull(ext.getPreambleDom());
        assertNotNull(ext.getPostambleDom());

        assertNotNull(step.getBodyDom());
        assertEquals(1, step.getBodyDom().getChildren().length);
        assertEquals("sparqlUpdate", step.getBodyDom().getChildren()[0].getName());
    }

    @Test
    void testParseForEachWithOptionalMessage() throws Exception {
        String xml =
                """
                <forEachItem>
                    <message>Processing items</message>
                    <item id="only">
                        <property name="x">val</property>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        ForEachItemStep step = ForEachItemStep.parse(buildConfig(xml));
        assertEquals("Processing items", step.getMessage());
    }

    @Test
    void testParseForEachMissingBodyThrows() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="core"><property name="x">y</property></item>
                </forEachItem>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> ForEachItemStep.parse(buildConfig(xml)),
                "Missing body should throw");
    }

    @Test
    void testParseForEachMissingItemsThrows() throws Exception {
        String xml =
                """
                <forEachItem>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> ForEachItemStep.parse(buildConfig(xml)),
                "No items should throw");
    }

    @Test
    void testParseItemMissingIdThrows() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item>
                        <property name="x">y</property>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> ForEachItemStep.parse(buildConfig(xml)),
                "Item without id should throw");
    }

    @Test
    void testParsePropertyMissingNameThrows() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="core">
                        <property>value</property>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> ForEachItemStep.parse(buildConfig(xml)),
                "Property without name should throw");
    }

    @Test
    void testParseUnknownItemChildThrows() throws Exception {
        String xml =
                """
                <forEachItem>
                    <item id="core">
                        <unknown>foo</unknown>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql>INSERT DATA { &lt;urn:s&gt; &lt;urn:p&gt; &lt;urn:o&gt; }</sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>
                """;
        assertThrows(
                ConfigurationParseException.class,
                () -> ForEachItemStep.parse(buildConfig(xml)),
                "Unknown element inside item should throw");
    }
}
