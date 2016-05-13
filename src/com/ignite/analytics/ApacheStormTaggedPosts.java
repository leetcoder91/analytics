package com.ignite.analytics;

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
 * Determine how many posts were tagged with Apache Storm.
 */
public class ApacheStormTaggedPosts {
	public static ArrayList<String> readNextBatch(LineIterator itr) {
		ArrayList<String> linesBatch = new ArrayList<String>();

		while (itr.hasNext() && linesBatch.size() < 100) {
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
		try (Ignite ignite = Ignition.start("Z:\\Nikhil\\Java\\stack-exchange-data\\ignite\\apache-ignite-fabric-1.5.0.final-bin\\apache-ignite-fabric-1.5.0.final-bin\\examples\\config\\example-ignite.xml")) {
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

		        calls.add(new IgniteCallable<Integer>() {
		            @Override
		            public Integer call() throws Exception {
		            	int count = 0;

		            	for (String post : posts) {
							Document doc = Jsoup.parse(post);
							Element body = doc.body();
							Elements row = body.select("row");
							String tags = row.attr("tags");

			                if ((tags.contains("<storm>") && tags.contains("<apache>")) || tags.contains("<apache-storm>")) {
            					count++;
							}
		            	}

		            	return count;
		            }
	            });

			    // Execute collection of Callables on the grid.
			    Collection<Integer> results = ignite.compute(remoteGroup).call(calls);

			    // Add up individual post counts received from remote nodes.
			    for (Integer result : results) {
			        postCount += result;
			    }

		        linesBatch = readNextBatch(itr);
		    }

			System.out.println(">>> Total number of posts that were tagged with Apache Storm is '" + postCount + "'.");
		}
	}

	public static void main(String[] args) {
		start(args[0]);
	}
}