package io.github.qudtlib.maven.rdfio.pipeline.step;

/**
 * Marker interface for steps whose {@link Step#execute} must be called before savepoint evaluation.
 *
 * <p>Steps implementing this interface are executed once, up front, on every run — before step
 * hashes are calculated and before a resume point is selected — and are then skipped by the main
 * execution loop. This is required for steps that only establish state needed by later steps
 * (rather than modifying the dataset), because resuming at a {@code <savepoint>} skips every
 * preceding step and would otherwise lose that state.
 *
 * <p>{@link StepDefStep} implements this: its {@code execute()} registers the step definition for
 * later use by {@link InvokeStep}, so without early execution an {@code <invoke>} following the
 * resume point would fail with "no &lt;stepDef&gt; has been registered".
 *
 * <p>Implementations must be idempotent and must not depend on dataset content, as they run before
 * any savepoint data is loaded.
 */
public interface ExecuteBeforeSavepointEvaluation extends Step {}
