package com.ignite.analytics;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.LineIterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.base.Strings;

/**
 * Count how many posts were made in July of 2014.
 */
public class PostsInJulyDistributed {
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

	@SuppressWarnings("serial")
	public static void start(String inputFilename) {
		try (Ignite ignite = Ignition.start("ignite.xml")) {
		    //ClusterGroup workers = ignite.cluster().forAttribute("ROLE", "worker");
		    ClusterGroup remoteGroup = ignite.cluster().forRemotes();

		    for (ClusterNode remoteNode : remoteGroup.nodes()) {
		    	System.out.println(remoteNode.id() + " " + remoteNode.hostNames() + " " + remoteNode.isLocal());
		    }

			PostsIterator producer = new PostsIterator();
			LineIterator itr = producer.getPostIterator(inputFilename);
			ArrayList<String> linesBatch = readNextBatch(itr);
			int postCount = 0;

			while (!linesBatch.isEmpty()) {
				Collection<IgniteCallable<Integer>> calls = new ArrayList<>();
				final ArrayList<String> posts = linesBatch;

            	for (final String post : posts) {
            		calls.add(new IgniteCallable<Integer>() {
            			@Override
            			public Integer call() throws Exception {
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

			    // Execute collection of Callables on the grid.
			    Collection<Integer> results = ignite.compute(remoteGroup).call(calls);

			    // Add up individual post counts received from remote nodes.
			    for (Integer result : results) {
			        postCount += result;
			    }

		        linesBatch = readNextBatch(itr);
		    }

		    System.out.println(">>> Total number of posts that were made in July of 2014 is '" + postCount + "'.");
		}
	}

	public static void main(String[] args) {
		start(args[0]);
	}
}
