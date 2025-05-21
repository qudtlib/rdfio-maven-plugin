package io.github.qudtlib.maven.rdfio.common.file;

public class ForbiddenFilePathException extends FileAccessException {
    public ForbiddenFilePathException() {}

    public ForbiddenFilePathException(String message) {
        super(message);
    }

    public ForbiddenFilePathException(String message, Throwable cause) {
        super(message, cause);
    }

    public ForbiddenFilePathException(Throwable cause) {
        super(cause);
    }

    public ForbiddenFilePathException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
