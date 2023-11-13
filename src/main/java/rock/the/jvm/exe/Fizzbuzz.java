package rock.the.jvm.exe;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class Fizzbuzz {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(1, 100);
        DataStream<Long> zeroToHunStream = env.fromSequence(1, 100);
        DataStream<Tuple2<Long, String>> fizzBuzzTuple2 = zeroToHunStream.map(num -> {
                            if (num % 3 == 0 && num % 5 == 0) return new Tuple2<>(num, "fizzbuzz");
                            if (num % 3 == 0) return new Tuple2<>(num, "fizz");
                            if (num % 5 == 0) return new Tuple2<>(num, "buzz");
                            return new Tuple2<>(num, "");
                        }
                ).returns(new TupleTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

        fizzBuzzTuple2
                .filter(tup -> tup.f1.equals("fizzbuzz"))
                .map(tup -> tup.f0)
                .sinkTo(new PrintSink<>());
        env.execute();
    }

}


