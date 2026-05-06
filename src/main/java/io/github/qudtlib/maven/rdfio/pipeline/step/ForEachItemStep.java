package io.github.qudtlib.maven.rdfio.pipeline.step;

import io.github.qudtlib.maven.rdfio.pipeline.Pipeline;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineHelper;
import io.github.qudtlib.maven.rdfio.pipeline.PipelineState;
import io.github.qudtlib.maven.rdfio.pipeline.step.support.ParsingHelper;
import io.github.qudtlib.maven.rdfio.pipeline.support.ConfigurationParseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.query.Dataset;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Iterates over a statically-defined list of named {@code <item>} configurations, executing
 * optional per-item {@code <preamble>} and {@code <postamble>} steps around a shared {@code
 * <body>}.
 *
 * <p>Within {@code <preamble>}, {@code <body>}, and {@code <postamble>}, the placeholder {@code
 * ${item.propertyName}} is substituted with the corresponding property value for the current item.
 * {@code ${item.id}} is always available.
 *
 * <p>Example:
 *
 * <pre>{@code
 * <forEachItem>
 *     <item id="core">
 *         <property name="suffix"></property>
 *         <property name="dataGraphs">dist:vocab:VOCAB_QUDT-UNITS-ALL.ttl</property>
 *         <postamble>
 *             <add><graph>inferred:factorUnits${item.suffix}</graph>
 *                  <toGraph>dist:vocab:VOCAB_QUDT-UNITS-ALL.ttl</toGraph></add>
 *         </postamble>
 *     </item>
 *     <item id="extensions">
 *         <property name="suffix">-ext</property>
 *         <property name="dataGraphs">dist:supported-extensions:vocab</property>
 *         <preamble><invoke stepRef="refresh-extension-aggregate"/></preamble>
 *     </item>
 *     <body>
 *         <shaclInfer>
 *             <message>Inferring qudt:hasFactorUnit${item.suffix}</message>
 *             ...
 *         </shaclInfer>
 *     </body>
 * </forEachItem>
 * }</pre>
 */
public class ForEachItemStep implements Step {

    /** A single named configuration item with per-item properties and optional step wrappers. */
    public static class Item {
        private String id;
        private final Map<String, String> properties = new LinkedHashMap<>();
        private Xpp3Dom preambleDom;
        private Xpp3Dom postambleDom;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Xpp3Dom getPreambleDom() {
            return preambleDom;
        }

        public void setPreambleDom(Xpp3Dom preambleDom) {
            this.preambleDom = preambleDom;
        }

        public Xpp3Dom getPostambleDom() {
            return postambleDom;
        }

        public void setPostambleDom(Xpp3Dom postambleDom) {
            this.postambleDom = postambleDom;
        }
    }

    private String message;
    private final List<Item> items = new ArrayList<>();
    private Xpp3Dom bodyDom;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<Item> getItems() {
        return items;
    }

    public void addItem(Item item) {
        this.items.add(item);
    }

    public Xpp3Dom getBodyDom() {
        return bodyDom;
    }

    public void setBodyDom(Xpp3Dom bodyDom) {
        this.bodyDom = bodyDom;
    }

    @Override
    public String getElementName() {
        return "forEachItem";
    }

    @Override
    public void execute(Dataset dataset, PipelineState state) throws MojoExecutionException {
        if (items.isEmpty()) {
            throw new MojoExecutionException("<forEachItem> requires at least one <item>");
        }
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new MojoExecutionException("<forEachItem> requires a non-empty <body>");
        }
        if (message != null) {
            state.log().info(message, 1);
        }
        state.incIndentLevel();
        try {
            for (Item item : items) {
                state.log().info("<forEachItem> item: " + item.getId());
                Map<String, String> subs = buildSubstitutions(item);
                if (item.getPreambleDom() != null) {
                    executeStepsDom(item.getPreambleDom(), subs, dataset, state, "preamble");
                }
                executeStepsDom(bodyDom, subs, dataset, state, "body");
                if (item.getPostambleDom() != null) {
                    executeStepsDom(item.getPostambleDom(), subs, dataset, state, "postamble");
                }
            }
        } finally {
            state.decIndentLevel();
        }
        state.getPrecedingSteps().add(this);
    }

    private static Map<String, String> buildSubstitutions(Item item) {
        Map<String, String> subs = new LinkedHashMap<>();
        subs.put("item.id", item.getId());
        for (Map.Entry<String, String> prop : item.getProperties().entrySet()) {
            subs.put("item." + prop.getKey(), prop.getValue());
        }
        return subs;
    }

    private static void executeStepsDom(
            Xpp3Dom containerDom,
            Map<String, String> subs,
            Dataset dataset,
            PipelineState state,
            String context)
            throws MojoExecutionException {
        Xpp3Dom substituted = substituteDom(containerDom, subs);
        for (Xpp3Dom stepDom : substituted.getChildren()) {
            Step step;
            try {
                step =
                        Pipeline.parseStep(
                                substituted,
                                stepDom,
                                stepDom.getName(),
                                "forEachItem",
                                "savepoint",
                                "stepDef");
            } catch (ConfigurationParseException e) {
                throw new MojoExecutionException(
                        "Error parsing <forEachItem> %s step <%s>: %s"
                                .formatted(context, stepDom.getName(), e.getMessage()),
                        e);
            }
            step.executeAndWrapException(dataset, state);
        }
    }

    /**
     * Deep-clones a Xpp3Dom tree, substituting {@code ${key}} placeholders in all text content and
     * attribute values.
     */
    static Xpp3Dom substituteDom(Xpp3Dom dom, Map<String, String> subs) {
        Xpp3Dom clone = new Xpp3Dom(dom.getName());
        for (String attrName : dom.getAttributeNames()) {
            clone.setAttribute(attrName, substitute(dom.getAttribute(attrName), subs));
        }
        if (dom.getValue() != null) {
            clone.setValue(substitute(dom.getValue(), subs));
        }
        for (Xpp3Dom child : dom.getChildren()) {
            clone.addChild(substituteDom(child, subs));
        }
        return clone;
    }

    private static String substitute(String input, Map<String, String> subs) {
        if (input == null || !input.contains("${")) {
            return input;
        }
        String result = input;
        for (Map.Entry<String, String> entry : subs.entrySet()) {
            result = result.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return result;
    }

    @Override
    public String calculateHash(String previousHash, PipelineState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(previousHash.getBytes(StandardCharsets.UTF_8));
            digest.update("forEachItem".getBytes(StandardCharsets.UTF_8));
            if (message != null) {
                digest.update(message.getBytes(StandardCharsets.UTF_8));
            }
            for (Item item : items) {
                digest.update(item.getId().getBytes(StandardCharsets.UTF_8));
                for (Map.Entry<String, String> prop : item.getProperties().entrySet()) {
                    digest.update(prop.getKey().getBytes(StandardCharsets.UTF_8));
                    digest.update(prop.getValue().getBytes(StandardCharsets.UTF_8));
                }
                if (item.getPreambleDom() != null) {
                    digest.update(
                            item.getPreambleDom().toString().getBytes(StandardCharsets.UTF_8));
                }
                if (item.getPostambleDom() != null) {
                    digest.update(
                            item.getPostambleDom().toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            if (bodyDom != null) {
                digest.update(bodyDom.toString().getBytes(StandardCharsets.UTF_8));
            }
            return PipelineHelper.serializeMessageDigest(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate hash", e);
        }
    }

    public static ForEachItemStep parse(Xpp3Dom config) {
        if (config == null) {
            throw new ConfigurationParseException(
                    config, "<forEachItem> configuration is missing.\n" + usage());
        }
        ForEachItemStep step = new ForEachItemStep();
        ParsingHelper.optionalStringChild(
                config, "message", step::setMessage, ForEachItemStep::usage);

        Xpp3Dom[] itemDoms = config.getChildren("item");
        if (itemDoms.length == 0) {
            throw new ConfigurationParseException(
                    config, "<forEachItem> requires at least one <item>.\n" + usage());
        }
        for (Xpp3Dom itemDom : itemDoms) {
            step.addItem(parseItem(itemDom));
        }

        Xpp3Dom bodyDom = config.getChild("body");
        if (bodyDom == null || bodyDom.getChildren().length == 0) {
            throw new ConfigurationParseException(
                    config, "<forEachItem> requires a <body> with at least one step.\n" + usage());
        }
        step.setBodyDom(bodyDom);

        return step;
    }

    private static Item parseItem(Xpp3Dom itemDom) {
        Item item = new Item();
        String id = itemDom.getAttribute("id");
        if (id == null || id.trim().isEmpty()) {
            throw new ConfigurationParseException(
                    itemDom, "<item> requires an 'id' attribute.\n" + usage());
        }
        item.setId(id.trim());

        for (Xpp3Dom child : itemDom.getChildren()) {
            switch (child.getName()) {
                case "property" -> {
                    String propName = child.getAttribute("name");
                    if (propName == null || propName.trim().isEmpty()) {
                        throw new ConfigurationParseException(
                                child, "<property> requires a 'name' attribute.\n" + usage());
                    }
                    String propValue = child.getValue() != null ? child.getValue().trim() : "";
                    item.getProperties().put(propName.trim(), propValue);
                }
                case "preamble" -> item.setPreambleDom(child);
                case "postamble" -> item.setPostambleDom(child);
                default ->
                        throw new ConfigurationParseException(
                                child,
                                "Unexpected element <%s> inside <item id=\"%s\">. Expected <property>, <preamble>, or <postamble>.\n%s"
                                        .formatted(child.getName(), item.getId(), usage()));
            }
        }
        return item;
    }

    public static String usage() {
        return """
                Usage: <forEachItem> with <item id="..."> children and a shared <body>.
                Each <item> may have <property name="propName">value</property> entries
                and optional <preamble> / <postamble> step lists.
                Within <preamble>, <body>, and <postamble>, ${item.propName} is substituted
                with the current item's property value. ${item.id} is always available.
                <savepoint> and <stepDef> are not allowed inside <forEachItem>.
                Example:
                <forEachItem>
                    <item id="core">
                        <property name="suffix"></property>
                    </item>
                    <item id="extensions">
                        <property name="suffix">-ext</property>
                        <preamble><invoke stepRef="refresh-extension-aggregate"/></preamble>
                    </item>
                    <body>
                        <sparqlUpdate>
                            <sparql><![CDATA[ INSERT { GRAPH <inferred:result${item.suffix}> { ?s ?p ?o } }
                            WHERE { ?s ?p ?o } ]]></sparql>
                        </sparqlUpdate>
                    </body>
                </forEachItem>""";
    }
}
