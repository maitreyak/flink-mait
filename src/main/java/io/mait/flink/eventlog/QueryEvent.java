package io.mait.flink.eventlog;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class QueryEvent {

    public static void main(String[] args) throws Exception {
        QueryableStateClient client = new QueryableStateClient("127.0.0.1", 9069);

// the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<String, String>> descriptor =
                new ValueStateDescriptor<>(
                        "logs",
                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                        }));

        CompletableFuture<ValueState<Tuple2<String, String>>> resultFuture =
                client.getKvState(JobID.fromHexString("620c3e42b96bd4625c94af730e843a10"), "logs", "271f94f4-bd76-4c4c-8956-0a4423d85eb9", BasicTypeInfo.STRING_TYPE_INFO, descriptor);

        Tuple2<String, String> res =  resultFuture.get().value();
        log.info("{}",res);
        return;

    }
}
