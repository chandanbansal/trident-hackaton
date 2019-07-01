package com.datasalt.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import java.io.IOException;


/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 * 
 * @author pere
 */
public class Skeleton {

	public static StormTopology buildTopology() throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
				.each(new Fields("id", "text", "actor", "location", "date"), new TridentUtils.PrintFilter("A"));
		return topology.build();
	}

	public static StormTopology buildTopology1() throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
				.each(new Fields("text", "actor"), new TridentUtils.UppercaseFunction(), new Fields("uppercased_text"))
				.each(new Fields("actor", "text", "uppercased_text"), new TridentUtils.PrintFilter("A"))
				.each(new Fields("actor", "text"), new TridentUtils.PerActorTweetsFilter("dave"))
				.each(new Fields("actor", "text"), new TridentUtils.PrintFilter("B"));
		return topology.build();
	}

	public static StormTopology buildTopology2() throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout", spout)
				.each(new Fields("actor", "text"), new TridentUtils.PrintFilter("A"))
				.peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.out.println(input.getStringByField("actor"));
                    }
                });

		return topology.build();
	}

    public static StormTopology buildTopologyMinBy() throws IOException {

        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("id", "actor", "text","location", "date"), new TridentUtils.PrintFilter("A"))
                .minBy("id")
                .each(new Fields("id", "actor"), new TridentUtils.PrintFilter("B"));
        return topology.build();
    }

    public static StormTopology buildTopologyPartitionAggregate() throws IOException {

        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .chainedAgg()
                .partitionAggregate(new Count(), new Fields("count"))
                .partitionAggregate(new Fields("id"), new Sum(), new Fields("sum"))
                .chainEnd()
                .peek(new TridentUtils.PrintConsumer());
        return topology.build();
    }

	public static void main(String[] args) throws Exception {

		Config conf = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopologyPartitionAggregate());
	}
}
