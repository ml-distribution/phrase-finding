package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 * 
 * Reprocesses the bigram data so the keys are the unigram terms, but the 
 * values include the bigram data so they can be reconstructed in the Reducer.
 */
public class JoinBigramMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		String bigram = key.toString();
		String[] grams = bigram.split(" ");
		String x = grams[0], y = grams[1];
		String[] counts = value.toString().split(",");
		long bgCount = Long.parseLong(counts[0]);
		long fgCount = Long.parseLong(counts[1]);

		// Send them out as unigrams.
		context.write(new Text(x), new Text(String.format("%s,%s,%s,x", bgCount, fgCount, bigram)));
		context.write(new Text(y), new Text(String.format("%s,%s,%s,y", bgCount, fgCount, bigram)));
	}

}
