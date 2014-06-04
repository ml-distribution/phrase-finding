package zx.soft.phrase.finding.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author wgybzb
 *
 */
public class PhrasesFinding extends Configured implements Tool {

	static enum PHRASE_COUNTERS {
		BG_TOTALWORDS, BG_TOTALPHRASES, BG_WORDVOCAB, BG_PHRASEVOCAB, FG_TOTALWORDS, FG_TOTALPHRASES, FG_WORDVOCAB, FG_PHRASEVOCAB
	}

	public static final String BG_TOTALWORDS = "phrase.finding.bg_totalwords";
	public static final String BG_TOTALPHRASES = "phrase.finding.bg_totalphrases";
	public static final String BG_WORDVOCAB = "phrase.finding.bg_wordvocab";
	public static final String BG_PHRASEVOCAB = "phrase.finding.bg_phrasevocab";
	public static final String FG_TOTALWORDS = "phrase.finding.fg_totalwords";
	public static final String FG_TOTALPHRASES = "phrase.finding.fg_totalphrases";
	public static final String FG_WORDVOCAB = "phrase.finding.fg_wordvocab";
	public static final String FG_PHRASEVOCAB = "phrase.finding.fg_phrasevocab";

	public static final String BG_CUTOFF = "phrase.finding.bg_cutoff";

	public static final String UNIGRAM = "unigram";
	public static final String BIGRAM = "bigram";
	public static final String FOREGROUND = "FG";
	public static final String BACKGROUND = "BG";

	public static final int NUMBEST = 20;

	private TreeMap<Double, String> scores;

	/**
	 * Deletes the specified path from HDFS so we can run this multiple times.
	 * @param conf
	 * @param path
	 */
	public static void delete(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	/**
	 * Handles organizing the final results to find the top 20 phrases.
	 * 
	 * @param score
	 * @param value
	 */
	public void add(double score, String value) {
		Double key = new Double(score);
		if (scores.floorKey(key) != null || scores.size() < PhrasesFinding.NUMBEST) {
			scores.put(key, value);

			// We've added something. Do we need to kick something out?
			if (scores.size() > PhrasesFinding.NUMBEST) {
				scores.remove(scores.firstKey());
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Configuration calculateConf = new Configuration();

		Path unigram = new Path(conf.get("unigram"));
		Path bigram = new Path(conf.get("bigram"));
		Path output = new Path(conf.get("output"));
		int numReducers = conf.getInt("reducers", 10);
		Path counts = new Path(output.getParent(), "counts");
		Path join = new Path(output.getParent(), "join");

		// Job 1: Determine foreground and background counts.
		PhrasesFinding.delete(conf, counts);
		Job countJob = new Job(conf, "shannon-phrases-count");
		countJob.setJarByClass(PhrasesFinding.class);
		countJob.setNumReduceTasks(numReducers);
		countJob.setMapperClass(InputDataMapper.class);
		countJob.setReducerClass(CountReducer.class);

		countJob.setInputFormatClass(TextInputFormat.class);
		countJob.setOutputFormatClass(TextOutputFormat.class);

		countJob.setMapOutputKeyClass(Text.class);
		countJob.setMapOutputValueClass(Text.class);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(countJob, unigram);
		FileInputFormat.addInputPath(countJob, bigram);
		FileOutputFormat.setOutputPath(countJob, counts);
		MultipleOutputs.addNamedOutput(countJob, PhrasesFinding.UNIGRAM, TextOutputFormat.class, Text.class,
				Text.class);
		MultipleOutputs.addNamedOutput(countJob, PhrasesFinding.BIGRAM, TextOutputFormat.class, Text.class,
				Text.class);

		if (!countJob.waitForCompletion(true)) {
			System.err.println("ERROR: Word training failed!");
			System.exit(1);
		}

		// Extract all the counters.
		calculateConf.setLong(PhrasesFinding.BG_PHRASEVOCAB,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.BG_PHRASEVOCAB).getValue());
		calculateConf.setLong(PhrasesFinding.BG_WORDVOCAB,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.BG_WORDVOCAB).getValue());
		calculateConf.setLong(PhrasesFinding.BG_TOTALPHRASES,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.BG_TOTALPHRASES).getValue());
		calculateConf.setLong(PhrasesFinding.BG_TOTALWORDS,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.BG_TOTALWORDS).getValue());
		calculateConf.setLong(PhrasesFinding.FG_PHRASEVOCAB,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.FG_PHRASEVOCAB).getValue());
		calculateConf.setLong(PhrasesFinding.FG_WORDVOCAB,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.FG_WORDVOCAB).getValue());
		calculateConf.setLong(PhrasesFinding.FG_TOTALPHRASES,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.FG_TOTALPHRASES).getValue());
		calculateConf.setLong(PhrasesFinding.FG_TOTALWORDS,
				countJob.getCounters().findCounter(PhrasesFinding.PHRASE_COUNTERS.FG_TOTALWORDS).getValue());

		// Job 2: Join the counts across terms.
		PhrasesFinding.delete(conf, join);
		Path bigramInput = new Path(counts, PhrasesFinding.BIGRAM + "*");
		Path unigramInput = new Path(counts, PhrasesFinding.UNIGRAM + "*");
		Job joinJob = new Job(conf, "shannon-phrases-join");
		joinJob.setJarByClass(PhrasesFinding.class);
		joinJob.setNumReduceTasks(numReducers);
		MultipleInputs.addInputPath(joinJob, unigramInput, KeyValueTextInputFormat.class, IdentityJoinMapper.class);
		MultipleInputs.addInputPath(joinJob, bigramInput, KeyValueTextInputFormat.class, JoinBigramMapper.class);
		joinJob.setReducerClass(JoinReducer.class);

		joinJob.setMapOutputKeyClass(Text.class);
		joinJob.setMapOutputValueClass(Text.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(joinJob, join);

		if (!joinJob.waitForCompletion(true)) {
			System.err.println("ERROR: Joining counts failed!");
			System.exit(1);
		}

		// Job 3: Mostly a continuation of Job 2. We now have 2 sets of counts
		// for each bigram; we need to unify them.
		PhrasesFinding.delete(calculateConf, output);
		Job calculateJob = new Job(calculateConf, "shannon-phrases-calculate");
		calculateJob.setJarByClass(PhrasesFinding.class);
		calculateJob.setNumReduceTasks(1); // forces a single output file
		calculateJob.setMapperClass(IdentityJoinMapper.class);
		calculateJob.setReducerClass(CalculateReducer.class);

		calculateJob.setInputFormatClass(KeyValueTextInputFormat.class);
		calculateJob.setOutputFormatClass(TextOutputFormat.class);

		calculateJob.setMapOutputKeyClass(Text.class);
		calculateJob.setMapOutputValueClass(Text.class);
		calculateJob.setOutputKeyClass(DoubleWritable.class);
		calculateJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(calculateJob, join);
		FileOutputFormat.setOutputPath(calculateJob, output);

		if (!calculateJob.waitForCompletion(true)) {
			System.err.println("ERROR: Calculating phrase stats failed!");
			System.exit(1);
		}

		// Final job: loop through the part-r-00000 file and tabulate the
		// largest 20 scores.
		scores = new TreeMap<Double, String>();
		FileSystem fs = output.getFileSystem(calculateConf);
		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(output, "part-r-00000"))));
		String line;
		while ((line = in.readLine()) != null) {
			String[] pieces = line.split("\t");
			double score = Double.parseDouble(pieces[0]);
			this.add(score, pieces[1]);
		}
		IOUtils.closeStream(in);

		// Print out the top 20!
		NavigableSet<Double> keys = scores.descendingKeySet();
		for (Double key : keys) {
			String[] elements = scores.get(key).split(",");
			String phrase = elements[0];
			double informativeness = Double.parseDouble(elements[1]);
			double phraseness = Double.parseDouble(elements[2]);
			System.out.println(String.format("%s\t%s\t%s\t%s", phrase, informativeness + phraseness, phraseness,
					informativeness));
		}
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PhrasesFinding(), args));
	}

}
