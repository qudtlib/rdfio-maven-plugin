package io.github.qudtlib.maven.rdfio.common;

import java.util.Collection;
import org.apache.maven.plugin.logging.Log;

public class LogHelper {

    private static String INDENT = "    ";

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

    public static void info(Log log, Collection<String> lines, int indentLevels) {
        if (log.isInfoEnabled()) {
            lines.forEach(line -> info(log, line, indentLevels));
        }
    }

    public static void info(Log log, String content, int indentLevels) {
        if (log.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < indentLevels; i++) {
                sb.append(INDENT);
            }
            sb.append(content);
            log.info(sb.toString());
        }
    }
}
