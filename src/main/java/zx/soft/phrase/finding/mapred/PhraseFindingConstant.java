package zx.soft.phrase.finding.mapred;

public class PhraseFindingConstant {

	static enum PHRASE_COUNTERS {
		BG_TOTAL_WORDS, BG_TOTAL_PHRASES, BG_WORD_VOCAB, BG_PHRASE_VOCAB, FG_TOTAL_WORDS, FG_TOTAL_PHRASES, FG_WORD_VOCAB, FG_PHRASE_VOCAB
	}

	public static final String BG_TOTAL_WORDS = "phrase.finding.bg_total_words";
	public static final String BG_TOTAL_PHRASES = "phrase.finding.bg_total_phrases";
	public static final String BG_WORD_VOCAB = "phrase.finding.bg_word_vocab";
	public static final String BG_PHRASE_VOCAB = "phrase.finding.bg_phrase_vocab";
	public static final String FG_TOTAL_WORDS = "phrase.finding.fg_total_words";
	public static final String FG_TOTAL_PHRASES = "phrase.finding.fg_total_phrases";
	public static final String FG_WORD_VOCAB = "phrase.finding.fg_word_vocab";
	public static final String FG_PHRASE_VOCAB = "phrase.finding.fg_phrase_vocab";

	public static final String BG_CUTOFF = "phrase.finding.bg_cutoff";

	public static final String UNIGRAM = "unigram";
	public static final String BIGRAM = "bigram";
	public static final String FORE_GROUND = "FG";
	public static final String BACK_GROUND = "BG";

	public static final int NUMBEST = 20;

}
