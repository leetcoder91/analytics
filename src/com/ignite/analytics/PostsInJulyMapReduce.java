package com.ignite.analytics;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
 * Count how many posts were made in July of 2014.
 */
public class PostsInJulyMapReduce {
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

	public static void start(String inputFilename) {
		Ignite ignite = Ignition.start("ignite.xml");
		IgniteCompute compute = ignite.compute();
		int postCount = 0;
		PostsIterator producer = new PostsIterator();
		LineIterator itr = producer.getPostIterator(inputFilename);
		ArrayList<String> linesBatch = readNextBatch(itr);

		while (!linesBatch.isEmpty()) { 
			// Execute task on the clustr and wait for its completion.
			postCount += compute.execute(PostsInJulyTask.class, linesBatch);

			linesBatch = readNextBatch(itr);
		}

		System.out.println(">>> Total number of posts that were made in July of 2014 is '" + postCount + "'.");

		Ignition.stop(true);
	}

	/**
	 * ComputeTaskSplitAdapter extends ComputeTaskAdapter and adds capability to automatically assign jobs to nodes.
	 * It hides the map(...) method and adds a new split(...) method in which user only needs to provide a collection
	 * of the jobs to be executed (the mapping of those jobs to nodes will be handled automatically by the adapter in
	 * a load-balanced fashion).
	 */
	private static class PostsInJulyTask extends ComputeTaskSplitAdapter<ArrayList<String>, Integer> {
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
						String creationDateStr = row.attr("CreationDate");
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

				    	try {
							if (!Strings.isNullOrEmpty(creationDateStr)) {
								long timestamp = df.parse(creationDateStr).getTime();

	            				// July 1, 2014 -> July 31, 2014
	            				if (timestamp >= 1404172800000l && timestamp <= 1406764800000l) {
	            					return 1;
	            				}
							}
						}
				    	catch (ParseException e) {
				    		e.printStackTrace();
						}

				    	return 0;
	            	}
	            });
			}

			return jobs;
		}
		
		@Override
		public Integer reduce(List<ComputeJobResult> results) {
			int postCount = 0;
			
			// Add up individual post counts received from remote nodes.
			for (ComputeJobResult result : results) {
				postCount += result.<Integer>getData();
			}

			return postCount;
		}
	}

	public static void main(String[] args) {
		start(args[0]);
	}
}
