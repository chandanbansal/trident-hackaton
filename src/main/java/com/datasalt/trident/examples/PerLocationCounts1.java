package com.datasalt.trident.examples;

import com.datasalt.trident.FakeTweetsBatchSpout;
import com.datasalt.trident.TridentUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This example shows the usage of aggregate() method for aggregating the WHOLE Trident batch of Tuples.
 * <p>
 * Because we aggregate the whole batch, we produce a hashmap with the counts per each location.
 * Aggregators are useful for processing Trident's batches. Note how we only use the collector at the end
 * of the Aggregator so we don't emit new Tuples for each Tuple that we process: we only emit one Tuple
 * per batch. For updating databases that's the best approach: you don't usually want to overload your DB
 * with one update per each Tuple. 
 *  
 * @author pere
 */
public class PerLocationCounts1 {
	
	@SuppressWarnings({ "serial" })
  public static class LocationAggregator extends BaseAggregator<Map<String, Integer>> {

		@Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
	    return new HashMap<String, Integer>();
    }

		@Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
			String location = tuple.getString(0);
			val.put(location, MapUtils.getInteger(val, location, 0) + 1);
    }

		@Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
			collector.emit(new Values(val));
    }
	}
	
	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(100);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
			.aggregate(new Fields("location"), new LocationAggregator(), new Fields("location_counts"))
			.each(new Fields("location_counts"), new TridentUtils.PrintFilter());
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));
	}
}
