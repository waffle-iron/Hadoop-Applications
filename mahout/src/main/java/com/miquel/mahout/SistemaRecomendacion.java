package com.github.mahout;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;

public class SistemaRecomendacion {

	private static final String COMA = ",";

	public static void main(String[] args) {

		
		try {
			// Cargamos el fichero con el dataset con valoraciones de usuarios a peliculas.
			DataModel model = new FileDataModel(new File(
					"/home/cloudera/Desktop/dataset.csv"), COMA);

			// Vamos a utilizar el coeficiente de correlacion de Pearson.
			PearsonCorrelationSimilarity similarity = new PearsonCorrelationSimilarity(
					model);

			// Utilizamos el algoritmo de vecinos mas cercanos. 
			UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.4,
					similarity, model);

			// Ahora ya podemos crear nuestro recomendador y entrenar el modelo.
			UserBasedRecommender recommender = new GenericUserBasedRecommender(
					model, neighborhood, similarity);

			// Solicitamos que 3 peliculas debemos recomendar al usuario 2.
			List<RecommendedItem> recommendations = recommender.recommend(2, 3);
			
			
			for (RecommendedItem recommendation : recommendations) {
				System.out.println(recommendation);
			}

		} catch (IOException | TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
