package com.ignite.analytics;

import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import java.util.TreeMap;

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
 * Determine what was the most popular month of Apache Storm questions (which month had most storm posts).
 */
public class MonthlyApacheStormTaggedPostsDistributed {
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
		    TreeMap<Integer, Integer> postCountByMonth = new TreeMap<Integer, Integer>();
		    int maxPostCount = Integer.MIN_VALUE;
		    int popularMonth = -1;

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
							String tags = row.attr("tags");
							String creationDateStr = row.attr("CreationDate");

							if (!Strings.isNullOrEmpty(creationDateStr) && !Strings.isNullOrEmpty(tags) &&
								((tags.contains("<storm>") && tags.contains("<apache>")) || tags.contains("<apache-storm>"))) {
								DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
								Date date = df.parse(creationDateStr);
								Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

						        cal.setTime(date);

					    		return cal.get(Calendar.MONTH);
							}

							return -1;
		            	}
		            });
	            }

			    // Execute collection of Callables on the grid.
			    Collection<Integer> months = ignite.compute(remoteGroup).call(calls);

			    // Add up individual post counts received from remote nodes.
			    for (Integer month : months) {
			    	if (month == null || month == -1) {
			    		continue;
			    	}

		    		Integer postCountForMonth = postCountByMonth.get(month);

		    		postCountForMonth = (postCountForMonth == null)	? 1 : (postCountForMonth + 1);

		    		postCountByMonth.put(month, postCountForMonth);

		    		if (maxPostCount < postCountForMonth) {
		    			maxPostCount = postCountForMonth;
		    			popularMonth = month;
		    		}
			    }

		        linesBatch = readNextBatch(itr);
		    }

		    if (popularMonth != -1) {
		    	System.out.println(">>> Most popular month of Apache Storm questions is '" + new DateFormatSymbols().getMonths()[popularMonth - 1] + "'.");
		    }
		}
	}

	public static void main(String[] args) {
		start(args[0]);
	}
}