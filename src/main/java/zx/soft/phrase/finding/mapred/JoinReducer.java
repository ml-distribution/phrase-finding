package zx.soft.phrase.finding.mapred;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Pulls together all the counts for a single phrase. Rather tricky. The key
 * is a unigram, but the list of values will contain both the unigram counts
 * AND the counts for that term within each bigram for which it is a constituent.
 * 
 * @author wgybzb
 *
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		long uniFG = 0, uniBG = 0;
		HashMap<String, String> bigramValues = new HashMap<String, String>();

		// First, we need to find the unigram counts. While we're at all,
		// we'll store all the information pertaining to each bigram that
		// the unigram is part of and loop through that later.
		for (Text value : values) {
			String[] elements = value.toString().split(",");
			if (elements.length > 2) {
				// Unigram is a bigram constituent.
				String bigram = elements[2];
				bigramValues.put(bigram, value.toString());
			} else {
				// Unigram is a unigram. Read the background and foreground counts.
				uniBG = Long.parseLong(elements[0]);
				uniFG = Long.parseLong(elements[1]);
			}
		}

		// Now that we have the unigram information (it occurs only once, but
		// it can be part of any number of bigrams), we'll loop through all the
		// bigrams in which it's a part and collate the information together.
		for (String bigram : bigramValues.keySet()) {
			String[] elements = bigramValues.get(bigram).split(",");
			long Bxy = Long.parseLong(elements[0]);
			long Cxy = Long.parseLong(elements[1]);
			boolean isX = elements[3].equals("x");

			long Bx = (isX ? uniBG : 0);
			long By = (isX ? 0 : uniBG);
			long Cx = (isX ? uniFG : 0);
			long Cy = (isX ? 0 : uniFG);

			// Write the output, indexing by the bigram.
			context.write(new Text(bigram), new Text(String.format("%s,%s,%s,%s,%s,%s", Bx, By, Cx, Cy, Bxy, Cxy)));
		}
	}

}
