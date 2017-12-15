package com.utils;

import org.apache.hadoop.mapred.Counters;

public class Contadores extends Counters {

	public enum CONTADOR {
		
		NUM_LINEAS_MAP,
		NUM_MAPPERS,
		NUM_REDUCERS,
		NUM_LINEAS_ESCRITAS,
		NUM_GRUPOS
		
	}
}
