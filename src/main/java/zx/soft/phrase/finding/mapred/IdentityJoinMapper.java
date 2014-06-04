package zx.soft.phrase.finding.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 * 
 * Essentially the identity mapper. Passes the key (unigram) and value 
 * (background and foreground counts) on to the reducer.
 */
public class IdentityJoinMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
