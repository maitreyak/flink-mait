package io.mait.flink.eventlog;


import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;


public class MaitCheckpointReader {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.getConfig().setExecutionMode(ExecutionMode.BATCH);
        SavepointReader savepoint = SavepointReader.read(env2, "file:///tmp/checkpoint-dir/f0b310e3c01895aca07e9a28fcc5078d/chk-10/_metadata", new HashMapStateBackend());
        savepoint.readListState(OperatorIdentifier.forUidHash("db8b842af5cea94c669b927a1d876377"),"streaming_committer_raw_states", TypeInformation.of(StreamRecord.class)).print();
        env2.execute("Batch read checkpoint job");
    }
}
