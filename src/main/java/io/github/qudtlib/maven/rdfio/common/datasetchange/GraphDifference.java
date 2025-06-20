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
                    return "unchanged graph: %s".formatted(name);
                } else {
                    if (sizeLeft == sizeRight) {
                        return "  changed graph: %s  (same size)".formatted(name);
                    } else {
                        long diff = sizeRight - sizeLeft;
                        return "  changed graph: %s  %s%d triple%s"
                                .formatted(
                                        name,
                                        diff > 0 ? "+" : "",
                                        diff,
                                        Math.abs(diff) != 1 ? "s" : "");
                    }
                }
            } else {
                return "  deleted graph: %s  had %d triple%s"
                        .formatted(name, sizeLeft, Math.abs(sizeLeft) != 1 ? "s" : "");
            }
        } else {
            if (existsInRight) {
                return "      new graph: %s  %d triple%s"
                        .formatted(name, sizeRight, Math.abs(sizeRight) != 1 ? "s" : "");
            } else {
                return "Mysterious graph %s exists neither in left nor in right - that's a bug"
                        .formatted(name);
            }
        }
    }
}
