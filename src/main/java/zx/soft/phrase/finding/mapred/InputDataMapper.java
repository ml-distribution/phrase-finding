package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads a line from the data files. It's fairly trivial to tell the differnece
 * between bigrams and unigrams, so we only need one mapper to consider them both.
 * 
 * @author wgybzb
 *
 */
public class InputDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	private int bgCutoff;

	@Override
	public void setup(Context context) {
		bgCutoff = context.getConfiguration().getInt(PhraseFindingConstant.BG_CUTOFF, 1970);
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
			type = PhraseFindingConstant.FORE_GROUND;
			if (isBigram) {
				context.getCounter(PhraseFindingConstant.PHRASE_COUNTERS.FG_TOTAL_PHRASES).increment(count);
			} else {
				context.getCounter(PhraseFindingConstant.PHRASE_COUNTERS.FG_TOTAL_WORDS).increment(count);
			}
		} else {
			// Background.
			type = PhraseFindingConstant.BACK_GROUND;
			if (isBigram) {
				context.getCounter(PhraseFindingConstant.PHRASE_COUNTERS.BG_TOTAL_PHRASES).increment(count);
			} else {
				context.getCounter(PhraseFindingConstant.PHRASE_COUNTERS.BG_TOTAL_WORDS).increment(count);
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
