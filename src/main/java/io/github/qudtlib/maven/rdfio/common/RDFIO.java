package io.github.qudtlib.maven.rdfio.common;

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

    /** Property relating a file URI to a graph URI (e.g., file://path loadsInto test:graph). */
    public static final Property loadsInto = ResourceFactory.createProperty(NS + "loadsInto");

    /** Property relating a file URI to a graph URI (e.g., file://path loadsInto test:graph). */
    public static final Property NoFile = ResourceFactory.createProperty(NS + "NoFile");

    /** Property for storing variable values (e.g., variables/fileGraph value "vocab:test1"). */
    public static final Property value = ResourceFactory.createProperty(NS + "value");

    /** Resource prefix for variables (e.g., variables/fileGraph). */
    public static final String VARIABLE_PREFIX = NS + "variables/";

    public static final Resource metadataGraph =
            ResourceFactory.createResource("rdfio:pipeline:metadata");
    public static final Resource shaclFunctionsGraph =
            ResourceFactory.createResource("rdfio:pipeline:userDefinedShaclFunctions");

    // Private constructor to prevent instantiation
    private RDFIO() {}

    public static Resource makeVariableUri(String var) {
        return ResourceFactory.createResource(VARIABLE_PREFIX + var);
    }
}
