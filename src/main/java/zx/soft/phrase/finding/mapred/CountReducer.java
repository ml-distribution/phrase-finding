package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Shannon Quinn
 * 
 * Accumulates counts for terms and saves unigrams and bigrams to different
 * output paths so they can be examined independently later.
 */
public class CountReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(Context context) throws InterruptedException, IOException {
		mos.close();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		String term = key.toString();
		String[] grams = term.split(" ");
		long bgCount = 0, fgCount = 0;

		// Iterate through the values and accumulate foreground/background counts.
		for (Text value : values) {
			String[] elements = value.toString().split(":");
			String type = elements[0];
			long count = Long.parseLong(elements[1]);
			if (type.equals(PhrasesController.FOREGROUND)) {
				fgCount += count;
			} else {
				bgCount += count;
			}
		}

		// Bigram.
		if (bgCount > 0 && grams.length == 2) {
			context.getCounter(PhrasesController.PHRASE_COUNTERS.BG_PHRASEVOCAB).increment(1);
		}
		if (fgCount > 0 && grams.length == 2) {
			context.getCounter(PhrasesController.PHRASE_COUNTERS.FG_PHRASEVOCAB).increment(1);
		}
		// Unigram.
		if (bgCount > 0 && grams.length == 1) {
			context.getCounter(PhrasesController.PHRASE_COUNTERS.BG_WORDVOCAB).increment(1);
		}
		if (fgCount > 0 && grams.length == 1) {
			context.getCounter(PhrasesController.PHRASE_COUNTERS.FG_WORDVOCAB).increment(1);
		}

		// Write the output. Name it according to whether it is unigram or bigram.
		mos.write((grams.length == 2 ? PhrasesController.BIGRAM : PhrasesController.UNIGRAM), key,
				new Text(String.format("%s,%s", bgCount, fgCount)));
	}

}
