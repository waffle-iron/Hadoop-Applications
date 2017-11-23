package com.miquel.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.utils.Contadores.CONTADOR;

import static com.openwebinars.mapreduce.Driver.SALIDA_PROCESO;

public class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {
	
	private static final Log LOG = LogFactory.getLog(Reduce.class);
	
	private MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(Context context) throws InterruptedException {
		
		mos = new MultipleOutputs<Text, Text>(context);
		context.getCounter(CONTADOR.NUM_REDUCERS).increment(1);
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		mos.close();
		
	}
	
	@Override
	public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context)
			throws IOException, InterruptedException {
		
		
		context.getCounter(CONTADOR.NUM_GRUPOS).increment(1);
		
		LOG.info("Ciudad :: " + key.toString());
		
		BigDecimal total = BigDecimal.ZERO;
		Long contador = 0l;
		
		for(final DoubleWritable temp : values) {
			
			total = total.add(BigDecimal.valueOf(temp.get()));
			contador ++;
			
//			LOG.info("Clave:" + key + "Value: " + temp);
		}
		
//		LOG.info("Valor total: " + total);
//		LOG.info("Contador: " + contador);
		
		BigDecimal media = total.divide(BigDecimal.valueOf(contador), 4, RoundingMode.HALF_UP);

//		LOG.info("Media: " + media);
		
		String salida = key.toString() + " : " + media.toString() + " ºC";
		
		mos.write(SALIDA_PROCESO, null, new Text(salida));
		context.getCounter(CONTADOR.NUM_LINEAS_ESCRITAS).increment(1);
		
	}
	
	
}
}
