package BigData.EvaluateRecommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class EvaluateRecommender {
	
	static int n_size;
	static String simIter;
	
	public static void main(String[] args) throws Exception{
		if (args.length != 2) {
		      System.err.println("Usage: EvaluateRecommender <input path> <output path>");
		      System.exit(-1);
		    }
		String[] simType = {"Pearson", "Euclidean", "Loglike"};
		String content = "n_size\tSimilarity\tMeasure\tError\tPrecision\tRecall\n";
		DataModel model = new FileDataModel(new File(args[0]));
		RecommenderBuilder builder = new EvaluateRecommender(). new MyRecommenderBuilder ();
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		for (String s: simType){
			simIter = s;
			for (n_size = 2; n_size <= 20; n_size += 2) {
//				RecommenderBuilder builder = new EvaluateRecommender(). new MyRecommenderBuilder ();
				double result = evaluator.evaluate(builder, null, model, 0.9, 1.0);
				content += Integer.toString(n_size) + "\t" + s + "\tMAE\t" 
						+ Double.toString(result) + "\tN\\A\tN\\A" +"\n";  
			}
		}	
		evaluator = new RMSRecommenderEvaluator();
		for (String s: simType){
			simIter = s;
			for (n_size = 2; n_size <= 20; n_size += 2) {
//				RecommenderBuilder builder = new EvaluateRecommender(). new MyRecommenderBuilder ();
				double result = evaluator.evaluate(builder, null, model, 0.9, 1.0);
				content += Integer.toString(n_size) + "\t" + s + "\tRMSE\t" 
						+ Double.toString(result) + "\tN\\A\tN\\A" +"\n";  
			}
		}
		RecommenderIRStatsEvaluator evaluatorIR = new GenericRecommenderIRStatsEvaluator();
		for (String s: simType){
			simIter = s;
			for (n_size = 2; n_size <= 20; n_size += 2) {
//				RecommenderBuilder builder = new EvaluateRecommender(). new MyRecommenderBuilder ();
				IRStatistics stats = evaluatorIR.evaluate(builder, null, model, null, 
						10, GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
				content += Integer.toString(n_size) + "\t" + s + "\tF Measure\tN\\A\t" 
						+ Double.toString(stats.getPrecision()) + "\t" 
						+ Double.toString(stats.getRecall()) + "\n";  
			}
		}	
		BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
		bw.write(content);
		System.out.println(content);
		bw.close();
	}
	class MyRecommenderBuilder implements RecommenderBuilder {
		
		public Recommender buildRecommender (DataModel model) throws TasteException {
			UserSimilarity similarity;
			switch (simIter){
			case "Pearson":
				similarity = new PearsonCorrelationSimilarity(model);
				break;
			case "Euclidean":
				similarity = new EuclideanDistanceSimilarity(model);
				break;
			case "Loglike":
				similarity = new LogLikelihoodSimilarity(model);
				break;
			default:
				System.out.println("Default Pearson is used");
				similarity = new PearsonCorrelationSimilarity(model);
				
			}
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(n_size, similarity, model);
			return new GenericUserBasedRecommender(model, neighborhood, similarity);
		}
	
	}	


}
