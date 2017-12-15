package com.miquel.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class HBase {

	private static final Log LOG = LogFactory.getLog(HBase.class);

	public static void main(String[] args) {

		try {

			// Creamos el objeto de configuracion. Por defecto, se usara la
			// configuracion existente en el fichero
			// hbase-site.xml
			Configuration conf = HBaseConfiguration.create();

			// Realizamos la conexion.
			Connection connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();

			Table tabla = null;
			// Creacion de datos
			if (!admin.tableExists(TableName.valueOf("pedidos"))) {
				// Creamos una nueva tabla.
				HTableDescriptor nuevaTabla = new HTableDescriptor(
						TableName.valueOf("pedidos"));

				// Anyadiamos dos nuevas columnas de familias.
				nuevaTabla.addFamily(new HColumnDescriptor("datosCliente"));
				nuevaTabla.addFamily(new HColumnDescriptor("datosPedido"));

				// Creamos la tabla.
				admin.createTable(nuevaTabla);

				tabla = connection.getTable(TableName.valueOf("pedidos"));

				Put p = new Put(Bytes.toBytes("fila1"));

				p.addColumn(Bytes.toBytes("datosCliente"),
						Bytes.toBytes("nombre"), Bytes.toBytes("Miquel"));

				p.addColumn(Bytes.toBytes("datosCliente"),
						Bytes.toBytes("apellido"), Bytes.toBytes("Andreu"));

				p.addColumn(Bytes.toBytes("datosPedido"),
						Bytes.toBytes("articulo"), Bytes.toBytes("reloj"));

				p.addColumn(Bytes.toBytes("datosPedido"),
						Bytes.toBytes("precio"), Bytes.toBytes("89,5"));

				tabla.put(p);
			}

			// Lectura de datos.
			if (tabla == null) {
				tabla = connection.getTable(TableName.valueOf("pedidos"));
			}

			// Recuperamos todos los datosde la fila que acabamos de insertar.
			Get fila1 = new Get(Bytes.toBytes("fila1"));
			Result result = tabla.get(fila1);

			// Ahora seleccionamos que datos queremos leer.
			byte[] nombres = result.getValue(Bytes.toBytes("datosCliente"),
					Bytes.toBytes("nombre"));
			byte[] articulos = result.getValue(Bytes.toBytes("datosPedido"),
					Bytes.toBytes("articulo"));
			byte[] precios = result.getValue(Bytes.toBytes("datosPedido"),
					Bytes.toBytes("precio"));

			LOG.info("Nombre: " + Bytes.toString(nombres) + " , articulo: "
					+ Bytes.toString(articulos) + " , precio: " + Bytes.toString(precios));

			
			//Borrado de datos.
			
			Delete del = new Delete(Bytes.toBytes("fila1"));
			del.addColumn(Bytes.toBytes("datosCliente"), Bytes.toBytes("apellido"));
			del.addFamily(Bytes.toBytes("datosPedido"));
			
			tabla.delete(del);
			
			tabla.close();

			
			//Eliminacion de tablas
			// 1. Deshabilitamos
			// 2. Eliminamos
			admin.disableTable(TableName.valueOf("pedidos"));
			admin.deleteTable(TableName.valueOf("pedidos"));
			
			
			//Cerramos la tabla y la conexion.
			connection.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
