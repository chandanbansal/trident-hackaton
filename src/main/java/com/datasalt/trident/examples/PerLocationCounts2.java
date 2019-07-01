package com.datasalt.trident.examples;

import com.datasalt.trident.FakeTweetsBatchSpout;
import com.datasalt.trident.TridentUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.io.IOException;

/**
 * This example illustrates the usage of groupBy. GroupBy creates a "grouped stream" which means that subsequent aggregators
 * will only affect Tuples within a group. GroupBy must always be followed by an aggregator. Because we are aggregating groups,
 * we don't need to produce a hashmap for the per-location counts (as opposed to {@link PerLocationCounts1} and we can use the 
 * simple Count() aggregator.
 * <p>
 * To better understand the different types of aggregations you can read: https://github.com/nathanmarz/storm/wiki/Trident-API-Overview .
 *  
 * @author pere
 */
public class PerLocationCounts2 {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(100);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
			.groupBy(new Fields("location"))
			.aggregate(new Fields("location"), new Count(), new Fields("count"))
			.each(new Fields("location", "count"), new TridentUtils.PrintFilter());
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}
