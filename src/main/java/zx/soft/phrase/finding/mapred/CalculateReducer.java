package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Calculate phraseness and informativeness.
 * 
 * @author wgybzb
 *
 */
public class CalculateReducer extends Reducer<Text, Text, DoubleWritable, Text> {

	/**
	 * Calculates the probability of x. Uses +1 smoothing (and hence requires
	 * the vocabulary size).
	 * 
	 * @param x
	 * @param total
	 * @param vocab
	 * @return
	 */
	public static double probability(long x, long total, long vocab) {
		return (x + 1.0) / ((double) total + (double) vocab);
	}

	/**
	 * Computes the informativeness of a phrase.
	 * 
	 * @param Bxy
	 * @param Cxy
	 * @param bgTotalPhrases
	 * @param bgPhraseVocab
	 * @param fgTotalPhrases
	 * @param fgPhraseVocab
	 * @return
	 */
	public static double informativeness(long Bxy, long Cxy, long bgTotalPhrases, long bgPhraseVocab,
			long fgTotalPhrases, long fgPhraseVocab) {
		double p = CalculateReducer.probability(Cxy, fgTotalPhrases, fgPhraseVocab);
		double q = CalculateReducer.probability(Bxy, bgTotalPhrases, bgPhraseVocab);
		return p * Math.log(p / q);
	}

	/**
	 * Calculates the "phraseness" of the phrase.
	 * 
	 * @param Cxy
	 * @param Cx
	 * @param Cy
	 * @param fgTotalPhrases
	 * @param fgPhraseVocab
	 * @param fgTotalWords
	 * @param fgWordVocab
	 * @return
	 */
	public static double phraseness(long Cxy, long Cx, long Cy, long fgTotalPhrases, long fgPhraseVocab,
			long fgTotalWords, long fgWordVocab) {
		double p = CalculateReducer.probability(Cxy, fgTotalPhrases, fgPhraseVocab);
		double q = CalculateReducer.probability(Cx, fgTotalWords, fgWordVocab)
				* CalculateReducer.probability(Cy, fgTotalWords, fgWordVocab);
		return p * Math.log(p / q);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		// Each list should be 2 items.
		long Cx = 0, Cy = 0, Bxy = 0, Cxy = 0;
		for (Text value : values) {
			String[] counts = value.toString().split(",");
			Cx += Long.parseLong(counts[2]);
			Cy += Long.parseLong(counts[3]);
			Bxy += Long.parseLong(counts[4]);
			Cxy += Long.parseLong(counts[5]);
		}

		// Make the computation.
		Configuration conf = context.getConfiguration();
		long fgTotalPhrases = conf.getLong(PhrasesFinding.FG_TOTALPHRASES, 0);
		long fgPhraseVocab = conf.getLong(PhrasesFinding.FG_PHRASEVOCAB, 0);
		long fgTotalWords = conf.getLong(PhrasesFinding.FG_TOTALWORDS, 0);
		long fgWordVocab = conf.getLong(PhrasesFinding.FG_WORDVOCAB, 0);
		long bgTotalPhrases = conf.getLong(PhrasesFinding.BG_TOTALPHRASES, 0);
		long bgPhraseVocab = conf.getLong(PhrasesFinding.BG_PHRASEVOCAB, 0);
		double phraseness = CalculateReducer.phraseness(Cxy, Cx, Cy, fgTotalPhrases, fgPhraseVocab, fgTotalWords,
				fgWordVocab);

		double informativeness = CalculateReducer.informativeness(Bxy, Cxy, bgTotalPhrases, bgPhraseVocab,
				fgTotalPhrases, fgPhraseVocab);

		context.write(new DoubleWritable(informativeness + phraseness),
				new Text(String.format("%s,%s,%s", key.toString(), informativeness, phraseness)));
		//context.write(key, new Text(String.format("%s,%s", informativeness, phraseness)));
	}

}
