package com.ignite.analytics;

import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.io.LineIterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.base.Strings;

/**
 * Determine what was the most popular month of Apache Storm questions (which month had most storm posts).
 */
public class MonthlyApacheStormTaggedPostsMapReduce {
	public static ArrayList<String> readNextBatch(LineIterator itr) {
		ArrayList<String> linesBatch = new ArrayList<String>();

		while (itr.hasNext() && linesBatch.size() < 10) {
			final String post = itr.nextLine();

			if (Strings.isNullOrEmpty(post)) {
				continue;
			}

			linesBatch.add(post);
		}

		return linesBatch;
	}

	/**
	 *   1. Read input file in batches of N lines
	 *   2. Creates a child job for each line
	 *   3. Sends created jobs to nodes in the grid for processing
	 */
	public static void start(String inputFilename) {
		Ignite ignite = Ignition.start("ignite.xml");
		IgniteCompute compute = ignite.compute();
	    TreeMap<Integer, Integer> postCountByMonth = new TreeMap<Integer, Integer>();
	    int maxPostCount = Integer.MIN_VALUE;
	    int popularMonth = -1;
		PostsIterator producer = new PostsIterator();
		LineIterator itr = producer.getPostIterator(inputFilename);
		ArrayList<String> linesBatch = readNextBatch(itr);

		while (!linesBatch.isEmpty()) { 
			// Execute task on the cluster and wait for its completion.
			TreeMap<Integer, Integer> partialPostCountByMonthMap =
				compute.execute(MonthlyApacheStormTaggedPostsTask.class, linesBatch);

		    // Add up individual post counts received from remote nodes.
	    	for (Map.Entry<Integer, Integer> partialPostCountByMonthEntry : partialPostCountByMonthMap.entrySet()) {
	    		Integer month = partialPostCountByMonthEntry.getKey();
	    		Integer partialPostCountPerMonth = partialPostCountByMonthEntry.getValue();
	    		Integer postCountPerMonth = postCountByMonth.get(month);

	    		postCountPerMonth = (postCountPerMonth == null)
    				? partialPostCountPerMonth : (postCountPerMonth + partialPostCountPerMonth);

	    		postCountByMonth.put(month, postCountPerMonth);

	    		if (maxPostCount < postCountPerMonth) {
	    			maxPostCount = postCountPerMonth;
	    			popularMonth = month;
	    		}
		    }

			linesBatch = readNextBatch(itr);
		}

	    if (popularMonth != -1) {
	    	System.out.println(">>> Most popular month of Apache Storm questions is '" + new DateFormatSymbols().getMonths()[popularMonth - 1] + "'.");
	    }

		Ignition.stop(true);
	}

	/**
	 * ComputeTaskSplitAdapter extends ComputeTaskAdapter and adds capability to automatically assign jobs to nodes.
	 * It hides the map(...) method and adds a new split(...) method in which user only needs to provide a collection
	 * of the jobs to be executed (the mapping of those jobs to nodes will be handled automatically by the adapter in
	 * a load-balanced fashion).
	 */
	private static class MonthlyApacheStormTaggedPostsTask extends ComputeTaskSplitAdapter<ArrayList<String>, TreeMap<Integer, Integer>> {
		@Override
		public Collection<? extends ComputeJob> split(int gridSize, final ArrayList<String> posts) {
			List<ComputeJob> jobs = new ArrayList<>();

			for (final String post : posts) {
				jobs.add(new ComputeJobAdapter() {
        			@Override
        			public Object execute() {
        				Document doc = Jsoup.parse(post);
						Element body = doc.body();
						Elements row = body.select("row");
						String tags = row.attr("tags");
						String creationDateStr = row.attr("CreationDate");

						if (!Strings.isNullOrEmpty(creationDateStr) && !Strings.isNullOrEmpty(tags) &&
							((tags.contains("<storm>") && tags.contains("<apache>")) || tags.contains("<apache-storm>"))) {
							DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

							try {
								Date date = df.parse(creationDateStr);

								Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

						        cal.setTime(date);

					    		return cal.get(Calendar.MONTH);
							}
							catch (ParseException e) {
								e.printStackTrace();
							}
						}

						return -1;
	            	}
	            });
			}

			return jobs;
		}
		
		@Override
		public TreeMap<Integer, Integer> reduce(List<ComputeJobResult> results) {
			TreeMap<Integer, Integer> postCountByMonth = new TreeMap<Integer, Integer>();

			// Add up individual post counts received from remote nodes.
			for (ComputeJobResult result : results) {
				Integer month = result.<Integer>getData();

		    	if (month == null || month == -1) {
		    		continue;
		    	}

	    		Integer postCountForMonth = postCountByMonth.get(month);

	    		postCountForMonth = (postCountForMonth == null)	? 1 : (postCountForMonth + 1);

	    		postCountByMonth.put(month, postCountForMonth);
		    }

			return postCountByMonth;
		}
	}

	public static void main(String[] args) {
		start(args[0]);
	}
}
