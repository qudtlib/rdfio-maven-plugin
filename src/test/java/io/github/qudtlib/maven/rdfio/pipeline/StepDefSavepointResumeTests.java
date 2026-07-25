package io.github.qudtlib.maven.rdfio.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test: a {@code <stepDef>} declared before a {@code <savepoint>} must still be
 * registered when the pipeline resumes at that savepoint, so that an {@code <invoke>} placed after
 * the savepoint can execute.
 *
 * <p>Before the fix, resuming skipped every step preceding the savepoint — including the {@code
 * <stepDef>}, whose {@code execute()} performs the registration — so the later {@code <invoke>}
 * failed with "no &lt;stepDef id="..."&gt; has been registered".
 */
public class StepDefSavepointResumeTests {

    private static final String CONFIG_FILE =
            "src/test/resources/pipeline-config-stepdef-savepoint.xml";

    private static final Path INVOKED_OUTPUT = Path.of("target/stepdef-savepoint-invoked.ttl");

    private File baseDir;

    @BeforeEach
    void setUp() throws Exception {
        baseDir = new File(".");
        Files.deleteIfExists(INVOKED_OUTPUT);
    }

    @Test
    void invokeAfterSavepointWorksWhenResuming() throws Exception {
        // First run: forceRun disables resuming, so the whole pipeline executes and the
        // <savepoint> is written.
        PipelineMojo firstRun = newMojo();
        firstRun.getPipeline().setForceRun(true);
        firstRun.execute();
        assertTrue(
                Files.exists(INVOKED_OUTPUT),
                "<invoke> should have executed during the initial (non-resuming) run");
        Files.delete(INVOKED_OUTPUT);

        // Second run: resumes at the <savepoint>, which sits after the <stepDef> and before the
        // <invoke>.
        PipelineMojo resumedRun = newMojo();
        resumedRun.getPipeline().setForceRun(false);
        resumedRun.execute();

        assertTrue(
                Files.exists(INVOKED_OUTPUT),
                "<invoke> should still execute after resuming from the savepoint");
        assertTrue(
                Files.readString(INVOKED_OUTPUT).contains("http://example.org/invoked"),
                "the triple inserted by the invoked <stepDef> should be present after resuming");
    }

    private PipelineMojo newMojo() throws Exception {
        PipelineMojo mojo = new PipelineMojo();
        mojo.setBaseDir(baseDir);
        mojo.setWorkBaseDir(new File("target"));
        setField(
                mojo,
                "configuration",
                Xpp3DomBuilder.build(Files.newInputStream(Path.of(CONFIG_FILE)), "UTF-8"));
        setField(mojo, "baseDir", baseDir);
        mojo.parseConfiguration();
        return mojo;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
