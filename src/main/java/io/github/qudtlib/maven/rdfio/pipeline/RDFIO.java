package io.github.qudtlib.maven.rdfio.pipeline;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Vocabulary for the RDF I/O Maven Plugin, defining properties and resources used in the metadata
 * graph.
 */
public final class RDFIO {

    /** Namespace for the RDF I/O vocabulary. */
    public static final String NS = "http://qudtlib.org/rdfio/";

    /** metadata graph string must be plain string as we use it in annotations * */
    public static final String metadataGraphString = "http://qudtlib.org/rdfio/metadata";

    /** Property relating a file URI to a graph URI (e.g., file://path loadsInto test:graph). */
    public static final Property loadsInto = ResourceFactory.createProperty(NS + "loadsInto");

    /** Property for storing variable values (e.g., variables/fileGraph value "vocab:test1"). */
    public static final Property value = ResourceFactory.createProperty(NS + "value");

    /** Resource prefix for variables (e.g., variables/fileGraph). */
    public static final String VARIABLE_PREFIX = NS + "variables/";

    public static final Resource metadataGraph =
            ResourceFactory.createResource(metadataGraphString);

    // Private constructor to prevent instantiation
    private RDFIO() {}

    public static Resource makeVariableUri(String var) {
        return ResourceFactory.createResource(VARIABLE_PREFIX + var);
    }
}
