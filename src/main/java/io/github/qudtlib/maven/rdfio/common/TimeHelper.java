package io.github.qudtlib.maven.rdfio.common;

public class TimeHelper {
    public static String makeDurationString(long duration) {
        if (duration < 1000) {
            return duration + "ms";
        }
        if (duration < 60000) {
            long s = duration / 1000;
            long ms = duration - (s * 1000);

            return s + "." + (ms / 100) + "s";
        }
        if (duration < 3600000) {
            long m = duration / 60000;
            long s = (duration - m * 60000) / 1000;
            return m + "m " + s + "s";
        }
        long h = duration / 3600000;
        long m = (duration - h * 3600000) / 60000;
        long s = (duration - h * 3600000 - m * 60000) / 1000;
        return h + "h " + m + "m " + s + "s";
    }
}
