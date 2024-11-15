package io.github.qudtlib.maven.rdfio.filter;

import org.apache.maven.plugin.logging.Log;

public abstract class AbstractFilter implements Filter {
    private Log log;

    public void setLog(Log log) {
        this.log = log;
    }

    protected Log getLog() {
        return this.log;
    }
}
