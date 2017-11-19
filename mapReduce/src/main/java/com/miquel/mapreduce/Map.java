package com.miquel.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.utils.Constantes;
import com.utils.Constantes.CAMPOS;
import com.utils.Contadores.CONTADOR;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static final Log LOG = LogFactory.getLog(Map.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		context.getCounter(CONTADOR.NUM_MAPPERS).increment(1);
		
		try {
			String linea = value.toString();
	
			String[] campos = linea.split(Constantes.COMA);
	
			if (!campos[CAMPOS.TEMPERATURA.ordinal()].isEmpty()) {
	
				context.write(
						new Text(campos[CAMPOS.CIUDAD.ordinal()]),
						new DoubleWritable(new Double(campos[CAMPOS.TEMPERATURA
								.ordinal()])));
				
			}
			
			context.getCounter(CONTADOR.NUM_LINEAS_MAP).increment(1);
			
		} catch (Exception e) {
			
			LOG.error("Error en el mapeo :: " + e.getMessage());
			
		}


	}

}