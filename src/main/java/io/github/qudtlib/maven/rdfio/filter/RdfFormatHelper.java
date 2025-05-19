package io.github.qudtlib.maven.rdfio.filter;

import org.apache.jena.riot.Lang;

public class RdfFormatHelper {
    public static boolean supportsQuads(Lang lang) {
        if (Lang.TRIG.equals(lang)
                || Lang.NQ.equals(lang)
                || Lang.NQUADS.equals(lang)
                || Lang.TRIX.equals(lang)) {
            return true;
        }
        return false;
    }
}
