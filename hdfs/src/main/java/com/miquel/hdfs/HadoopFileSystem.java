package com.miquel.hdfs;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HadoopFileSystem {

	private static String rutaHDFS = "/test";
	private static String contenido1 = "Hola Mundo";
	private static String nombreFichero1 = "Fichero1";
	
	private static String rutaLocal =  "/home/cloudera/test";
	private static String ficheroLocal = "/home/cloudera/test/Local";
	
	
	public static void main(String[] args) {

		try {
			
			//Creamos la configuracion del HDFS
			Configuration conf = new Configuration(true);
			conf.set("fs.defaultFS", "hdfs://localhost:8020/user/hdfs/");
			
			System.setProperty("HADOOP_USER_NAME", "hdfs");
			System.setProperty("hadoop.home.dir", "/");

			//Creamos el objecto FileSystem.
			FileSystem fs = FileSystem.get(conf);

			//En caso de no existir el directorio, lo creamos.
			if (!fs.exists(new Path(rutaHDFS))) {
				fs.mkdirs(new Path(rutaHDFS));
			}
			
			// Si no existe el fichero lo generamos.
			Path rutaFichero1 = new Path(rutaHDFS + "/" + nombreFichero1);
			FSDataOutputStream outputStream = null;
			
			if(!fs.exists(rutaFichero1)) {
				outputStream = fs.create(rutaFichero1);
				outputStream.writeBytes(contenido1);
				outputStream.close();
			}
			
			
			//Vamos a leer el fichero que acabamos de escribir.
			Path rutaFichero = new Path(rutaHDFS + "/" + nombreFichero1);

			FSDataInputStream inputStream = fs.open(rutaFichero);
			String salida = IOUtils.toString(inputStream, "UTF-8");
			inputStream.close();

			System.out.println(salida);
			
			//Podemos ver el estado del fichero.
			FileStatus status = fs.getFileStatus(rutaFichero1);
			
			//Tambien podemos modificar el propietario o los permisos del fichero.
			fs.setOwner(rutaFichero1, "cloudera", "cloudera");
			
			FsPermission permisos = FsPermission.valueOf("-rwxrwxrwx");
			fs.setPermission(rutaFichero1, permisos);
			
			//Al igual que hemos realizado con la línea de comandos, podemos mover fichero de Local al HDFS y viceversa.
			//Local --> HDFS
			fs.copyFromLocalFile(false, true, new Path(ficheroLocal), new Path(rutaHDFS));
			//HDFS --> Local
			fs.copyToLocalFile(false, rutaFichero1, new Path(rutaLocal));
			
			//Por ultimo, borraremos el directorio y los ficheros.
			fs.delete(new Path(rutaHDFS), true);
			
			fs.close();
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}

	}

}