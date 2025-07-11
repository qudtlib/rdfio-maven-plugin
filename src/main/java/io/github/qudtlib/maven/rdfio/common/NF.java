package io.github.qudtlib.maven.rdfio.common;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class NF {
    /** Namespace for the RDF I/O vocabulary. */
    public static final String NS = "https://github.com/qudtlib/numericFunctions/";

    /** Property relating a file URI to a graph URI (e.g., file://path loadsInto test:graph). */
    public static final Resource decimal_div = ResourceFactory.createProperty(NS + "decimal.div");

    public static final Resource decimal_pow = ResourceFactory.createProperty(NS + "decimal.pow");

    public static final Resource decimal_precision =
            ResourceFactory.createProperty(NS + "decimal.precision");
    public static final Resource decimal_roundToPrecision =
            ResourceFactory.createProperty(NS + "decimal.roundToPrecision");
}
