package io.github.qudtlib.maven.rdfio.common.datasetchange;

public record GraphDifference(
        String name,
        boolean existsInLeft,
        long sizeLeft,
        boolean existsInRight,
        long sizeRight,
        boolean graphsAreEqual) {

    public String formatForChange() {
        if (existsInLeft) {
            if (existsInRight) {
                if (graphsAreEqual) {
                    return "Graph '%s': no change".formatted(name);
                } else {
                    if (sizeLeft == sizeRight) {
                        return "Graph '%s: changed, same size".formatted(name);
                    } else {
                        long diff = sizeRight - sizeLeft;
                        return "Graph '%s': changed, %s%d triples"
                                .formatted(name, diff > 0 ? "+" : "", diff);
                    }
                }
            } else {
                return "Graph '%s': deleted (had %d triples)".formatted(name, sizeLeft);
            }
        } else {
            if (existsInRight) {
                return "Graph '%s': added (%d triples)".formatted(name, sizeRight);
            } else {
                return "Mysterious graph '%s' exists neither in left nor in right - that's a bug"
                        .formatted(name);
            }
        }
    }
}
