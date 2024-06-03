package butterfly.example.jmhpractice;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author Lynn Lee
 * @date 2023/11/29
 **/
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class stringTest {

    @Benchmark
    public String StringAdd() {
        String result = "";
        for (int i = 0; i < 10; i++) {
            result += i;
        }
        return result;
    }

    @Benchmark
    public StringBuilder StringBuilderAdd() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            stringBuilder.append(i);
        }
        return stringBuilder;
    }

    @Benchmark
    public StringBuffer StringBufferAdd() {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            stringBuffer.append(i);
        }
        return stringBuffer;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(stringTest.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .result("StringPerformance.json")
                .build();
        new Runner(options).run();
    }
}
