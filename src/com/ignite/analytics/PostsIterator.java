package com.ignite.analytics;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PostsIterator {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(PostsIterator.class);

	// Operations

	public File getFile(String filename) throws IOException {
		File input = new File(filename);

		if (!input.exists()) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("\"" + filename + "\" does not exist.");
			}

			return null;			
		}
		else if (!input.canRead()) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("\"" + filename + "\" is read protected.");
			}

			return null;
		}

		return input;
	}

	public LineIterator getPostIterator(String filename) {
		try {
			File inputFile = getFile(filename);

			return FileUtils.lineIterator(inputFile, "UTF-8");
		}
		catch (IOException e) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("Failed to process file \"" + filename + "\". Skipping file.", e);
			}
		}

		return null;
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: java PostsIterator file");

			System.exit(1);
		}

		PostsIterator producer = new PostsIterator();

		for (LineIterator itr = producer.getPostIterator(args[0]); itr.hasNext();) {
			System.out.println(itr.next());
		}
	}
}
