package io.github.qudtlib.maven.rdfio.common.file;

public class RelativePathException extends RuntimeException {
    public RelativePathException() {}

    public RelativePathException(String message) {
        super(message);
    }

    public RelativePathException(String message, Throwable cause) {
        super(message, cause);
    }

    public RelativePathException(Throwable cause) {
        super(cause);
    }

    public RelativePathException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
