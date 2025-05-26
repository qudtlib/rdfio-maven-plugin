package io.github.qudtlib.maven.rdfio.common;

import java.util.Collection;
import org.apache.maven.plugin.logging.Log;

public class LogHelper {

    public static void debug(Log log, Collection<String> lines) {
        if (log.isDebugEnabled()) {
            lines.forEach(log::debug);
        }
    }

    public static void info(Log log, Collection<String> lines) {
        if (log.isInfoEnabled()) {
            lines.forEach(log::info);
        }
    }
}
