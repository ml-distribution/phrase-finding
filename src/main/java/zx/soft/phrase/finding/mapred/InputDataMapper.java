package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 *
 * Reads a line from the data files. It's fairly trivial to tell the differnece
 * between bigrams and unigrams, so we only need one mapper to consider them both.
 */
public class InputDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	private int bgCutoff;

	@Override
	public void setup(Context context) {
		bgCutoff = context.getConfiguration().getInt(PhrasesController.BG_CUTOFF, 1970);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] pieces = value.toString().split("\t");
		String term = pieces[0];
		boolean isBigram = term.split(" ").length == 2;
		int decade = Integer.parseInt(pieces[1]);
		long count = Long.parseLong(pieces[2]);

		// Increment requisite counters.
		String type = null;
		if (decade < bgCutoff) { // yes: BEFORE 1970 is the CORPUS, AFTER is the BACKGROUND
			// Foreground.
			type = PhrasesController.FOREGROUND;
			if (isBigram) {
				context.getCounter(PhrasesController.PHRASE_COUNTERS.FG_TOTALPHRASES).increment(count);
			} else {
				context.getCounter(PhrasesController.PHRASE_COUNTERS.FG_TOTALWORDS).increment(count);
			}
		} else {
			// Background.
			type = PhrasesController.BACKGROUND;
			if (isBigram) {
				context.getCounter(PhrasesController.PHRASE_COUNTERS.BG_TOTALPHRASES).increment(count);
			} else {
				context.getCounter(PhrasesController.PHRASE_COUNTERS.BG_TOTALWORDS).increment(count);
			}
		}

		// Write the output.
		if (isBigram) {
			String[] grams = term.split(" ");
			context.write(new Text(grams[0]), new Text(String.format("%s:%s", type, count)));
			context.write(new Text(grams[1]), new Text(String.format("%s:%s", type, count)));
		}
		context.write(new Text(term), new Text(String.format("%s:%s", type, count)));
	}

}
