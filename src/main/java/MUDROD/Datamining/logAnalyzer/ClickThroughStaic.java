package MUDROD.Datamining.logAnalyzer;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClickThroughStaic implements Serializable {

	public String query;
	public String dataset;
	public int num;
	// public boolean bDownload;
	public List<String> keywords;

	public int keywordsId;

	public ClickThroughStaic() {
		keywords = new ArrayList<String>();
	}

	public List<String> getContext() {
		return this.keywords;
	}

	public List<String> getUniqueKeywords() {
		List<String> uniqueKeywords = new ArrayList<String>();
		HashSet h = new HashSet(this.keywords);
		uniqueKeywords.addAll(h);
		return uniqueKeywords;
	}

	public int getNum() {
		return this.num;
	}

	public String getQuery() {
		return this.query;
	}

	public ClickThroughStaic(String query, String dataset, int num) {
		this.query = query;
		this.dataset = dataset;
		this.num = num;

		this.keywords = new ArrayList<String>();

	}

	public void setKeywords(String keywords) {
		if (keywords != null) {
			String words[] = keywords.split(",");
			if (words != null || words.length > 0) {
				for (int i = 0; i < words.length; i++) {
					String word = words[i].trim();
					if (word.startsWith("\"")) {
						word = word.substring(1);
					}
					if (word.endsWith("\"")) {
						word = word.substring(0, word.length() - 1);
					}
					this.keywords.add(word);
				}
			}
		}
	}

	public static ClickThroughStaic getContext(ClickThroughStaic s, Map<String, String> datakeywords) {

		String keywordStr = datakeywords.get(s.dataset);
		s.setKeywords(keywordStr);

		return s;
	}

	public String calMD5(String str) throws NoSuchAlgorithmException {

		MessageDigest md5 = MessageDigest.getInstance("MD5");
		byte[] bs = md5.digest(str.getBytes());
		StringBuilder sb = new StringBuilder(40);
		for (byte x : bs) {
			if ((x & 0xff) >> 4 == 0) {
				sb.append("0").append(Integer.toHexString(x & 0xff));
			} else {
				sb.append(Integer.toHexString(x & 0xff));
			}
		}
		return sb.toString();
	}

	public static Vector toVector(ClickThroughStaic stat, int querynums, Map<String, Integer> keywordQueryNum) {
		List<String> keywords = stat.getContext();

		String query = stat.getQuery();
		Set<String> allkeywords = keywordQueryNum.keySet();
		double[] array = new double[allkeywords.size()];
		int i = 0;
		for (String keyword : allkeywords) {
			if (keywords.contains(keyword)) {
				// String keyword = keywords.get(i);
				Double tf = 1.0 / keywords.size();
				Double idf = (double) (querynums / keywordQueryNum.get(keyword));
				Double rate = tf * Math.log(idf);

				array[i] = rate;
			} else {
				array[i] = 0;

			}
			i += 1;
		}

		Vector currentRow = Vectors.dense(array);
		return currentRow;
	}

}
