package com.datasalt.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Misc. util classes that can be used for implementing some stream processing examples.
 *
 * @author pere
 */
public class TridentUtils {

	/**
	 * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
	 */
	@SuppressWarnings({"serial", "rawtypes"})
	public static class PrintFilter implements Filter {
		private int partitionIndex;
		private String prefix = "";

		public PrintFilter() {
		}

		public PrintFilter(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
			System.out.println("Total number of partions in PrintFilter[" + prefix + "]:" + context.numPartitions());
		}

		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println("[" + this.prefix + "]PrintFilter, I am partition [" + partitionIndex + "] -- " + tuple);
			return true;
		}
	}

	/**
	 * A filter that filters the tuples passed in constructor.
	 */
	public static class PerActorTweetsFilter extends BaseFilter {
		String actor;
		private int partitionIndex;

		public PerActorTweetsFilter(String actor) {
			this.actor = actor;
		}

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
			System.out.println("Total number of partiions:" + context.numPartitions());
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			boolean filter = tuple.getString(0).equals(actor);
			if (filter) {
				System.err.println("I am partition [" + partitionIndex + "] and I have kept a tweet by: " + actor);
			}
			return filter;
		}
	}

	/**
	 * A function that will upper case the 0 index field.
	 */
	public static class UppercaseFunction extends BaseFunction {
		private int partitionIndex;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
			System.out.println("Total number of partions in UppercaseFunction:" + context.numPartitions());
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("###UppercaseFunction, I am partition [" + partitionIndex + "] -- " + tuple);
			collector.emit(new Values(tuple.getString(0).toUpperCase()));
		}
	}

	public static class PrintConsumer implements Consumer {
		@Override
		public void accept(TridentTuple input) {
			System.out.println(input);
		}
	}

	/**
	 * Given a hashmap with string keys and integer counts, returns the "top" map of it. "n" specifies the size of
	 * the top to return.
	 */
	public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n) {
		List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
		entryList.addAll(map.entrySet());
		Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});
		Map<String, Integer> toReturn = new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : entryList.subList(0, Math.min(entryList.size(), n))) {
			toReturn.put(entry.getKey(), entry.getValue());
		}
		return toReturn;
	}


}
