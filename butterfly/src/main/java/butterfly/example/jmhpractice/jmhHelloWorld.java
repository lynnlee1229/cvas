package butterfly.example.jmhpractice;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * @author Lynn Lee
 * @date 2023/11/30
 **/
public class jmhHelloWorld {
    @Benchmark
    public void helloWorld() {
    }

    public static void main(String[] args) throws RunnerException {
        Options build = new OptionsBuilder()
                .include(jmhHelloWorld.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(build).run();

    }
}


