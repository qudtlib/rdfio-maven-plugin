package io.github.qudtlib.maven.rdfio.common.log;

import java.util.Arrays;
import java.util.Objects;
import org.apache.maven.plugin.logging.Log;

public class StdoutLog implements Log {
    public enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    private Level level = Level.DEBUG;

    private int levelIndex = levelIndex(level);
    private static int debugIndex = levelIndex(Level.DEBUG);
    private static int infoIndex = levelIndex(Level.INFO);
    private static int warnIndex = levelIndex(Level.WARN);
    private static int errorIndex = levelIndex(Level.ERROR);

    private static int levelIndex(Level level) {
        Level[] values = Level.values();
        for (int i = 0; i < values.length; i++) {
            if (values[i] == level) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Level %s not found in levels %s"
                        .formatted(level.toString(), Arrays.toString(Level.values())));
    }

    public void setLevelDebug() {
        setLevel(Level.DEBUG);
    }

    public void setLevelInfo() {
        setLevel(Level.INFO);
    }

    public void setLevelWarn() {
        setLevel(Level.WARN);
    }

    public void setLevelError() {
        setLevel(Level.ERROR);
    }

    public void setLevel(Level level) {
        Objects.requireNonNull(level);
        this.level = level;
        this.levelIndex = levelIndex(level);
    }

    @Override
    public boolean isDebugEnabled() {
        return (levelIndex <= debugIndex);
    }

    @Override
    public void debug(CharSequence content) {
        if (isDebugEnabled()) {
            System.out.println("DEBUG: " + content);
        }
    }

    @Override
    public void debug(CharSequence content, Throwable error) {
        if (isDebugEnabled()) {
            System.out.println("DEBUG: " + content);
            error.printStackTrace(System.out);
        }
    }

    @Override
    public void debug(Throwable error) {
        if (isDebugEnabled()) {
            System.out.println("DEBUG:");
            error.printStackTrace(System.out);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return (levelIndex <= infoIndex);
    }

    @Override
    public void info(CharSequence content) {
        if (isInfoEnabled()) {
            System.out.println("INFO: " + content);
        }
    }

    @Override
    public void info(CharSequence content, Throwable error) {
        if (isInfoEnabled()) {
            System.out.println("INFO: " + content);
            error.printStackTrace(System.out);
        }
    }

    @Override
    public void info(Throwable error) {
        if (isInfoEnabled()) {
            System.out.println("INFO:");
            error.printStackTrace(System.out);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return (levelIndex <= warnIndex);
    }

    @Override
    public void warn(CharSequence content) {
        if (isWarnEnabled()) {
            System.out.println("WARN: " + content);
        }
    }

    @Override
    public void warn(CharSequence content, Throwable error) {
        if (isWarnEnabled()) {
            System.out.println("WARN: " + content);
            error.printStackTrace(System.out);
        }
    }

    @Override
    public void warn(Throwable error) {
        if (isWarnEnabled()) {
            System.out.println("WARN:");
            error.printStackTrace(System.out);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return (levelIndex <= errorIndex);
    }

    @Override
    public void error(CharSequence content) {
        if (isErrorEnabled()) {
            System.out.println("ERROR: " + content);
        }
    }

    @Override
    public void error(CharSequence content, Throwable error) {
        if (isErrorEnabled()) {
            System.out.println("ERROR: " + content);
            error.printStackTrace(System.out);
        }
    }

    @Override
    public void error(Throwable error) {
        if (isErrorEnabled()) {
            System.out.println("ERROR:");
            error.printStackTrace(System.out);
        }
    }
}
