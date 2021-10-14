package io.deephaven.engine.v2.sources.chunk;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BenchmarkEval {

    /*
     * This is our first benchmark method.
     *
     * JMH works as follows: users annotate the methods with @Benchmark, and
     * then JMH produces the generated code to run this particular benchmark as
     * reliably as possible. In general one might think about @Benchmark methods
     * as the benchmark "payload", the things we want to measure. The
     * surrounding infrastructure is provided by the harness itself.
     *
     * Read the Javadoc for @Benchmark annotation for complete semantics and
     * restrictions. At this point we only note that the methods names are
     * non-essential, and it only matters that the methods are marked with
     * @Benchmark. You can have multiple benchmark methods within the same
     * class.
     *
     * Note: if the benchmark method never finishes, then JMH run never finishes
     * as well. If you throw an exception from the method body the JMH run ends
     * abruptly for this benchmark and JMH will run the next benchmark down the
     * list.
     *
     * Although this benchmark measures "nothing" it is a good showcase for the
     * overheads the infrastructure bear on the code you measure in the method.
     * There are no magical infrastructures which incur no overhead, and it is
     * important to know what are the infra overheads you are dealing with. You
     * might find this thought unfolded in future examples by having the
     * "baseline" measurements to compare against.
     */

    int x = 0;
    int i = 0;

    @Setup(Level.Invocation)
    public void updateI() {

        i++;
    }

    @Benchmark
    public double wellHelloThere() {
        return 0;//x + (i % 47) ^ 0xDEADBEEF;
    }

    /*
     * ============================== HOW TO RUN THIS TEST: ====================================
     *
     * You are expected to see the run with large number of iterations, and
     * very large throughput numbers. You can see that as the estimate of the
     * harness overheads per method call. In most of our measurements, it is
     * down to several cycles per call.
     *
     * a) Via command-line:
     *    $ mvn clean install
     *    $ java -jar target/benchmarks.jar JMHSample_01
     *
     * JMH generates self-contained JARs, bundling JMH together with it.
     * The runtime options for the JMH are available with "-h":
     *    $ java -jar target/benchmarks.jar -h
     *
     * b) Via the Java API:
     *    (see the JMH homepage for possible caveats when running from IDE:
     *      http://openjdk.java.net/projects/code-tools/jmh/)
     */

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkEval.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
//TODO
}
