package zx.soft.phrase.finding.driver;

import org.apache.hadoop.util.ProgramDriver;

import zx.soft.phrase.finding.mapred.PhrasesFindingDistribute;

public class PhraseFindingDriver {

	public static void main(String argv[]) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("phrasesFindingDistribute", PhrasesFindingDistribute.class, "Phrase-Finding分布式实现");
			pgd.driver(argv);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
