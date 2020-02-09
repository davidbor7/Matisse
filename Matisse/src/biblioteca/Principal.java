package biblioteca;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.matisse.MtDatabase;
import com.matisse.MtException;
import com.matisse.MtObjectIterator;
import com.matisse.MtPackageObjectFactory;

public class Principal
{


	public static void main(String[] args)
	{

		String hostname = "localhost";
		String dbname = "biblioteca";
		creaObjetos(hostname, dbname);
		//borrarTodos(hostname, dbname);
		//modificaObjeto(hostname, dbname, "David", 22);
		//ejecutaOQL(hostname, dbname);
	}


	public static void creaObjetos(String hostname, String dbname) 
	{
		try 
		{
			// Abre la base de datos con el Hostname (localhost), dbname (biblioteca) y el namespace "biblioteca".

			MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
			db.open();
			db.startTransaction();
			System.out.println("Conectado a la base de datos " + db.toString() + " de Matisse");


			// Crea un objeto Autor
			Autor autor = new Autor(db);
			autor.setNombre("David");
			autor.setApellidos("Borrego");
			autor.setEdad(25);
			System.out.println("Autor "+ autor.getNombre() + " " + autor.getApellidos() +" creado con éxito");

			
			// Crea un objeto Libro
			Libro l1 = new Libro(db);
			l1.setTitulo("La sombra del Viento");
			l1.setEditorial("Planeta");
			l1.setPaginas(1050);
			System.out.println("Libro creado con éxito...");

			// Crea otro objeto Libro
			Libro l2 = new Libro(db);
			l2.setTitulo("Harry Potter");
			l2.setEditorial("Salamandra");
			l2.setPaginas(798);
			System.out.println("Libro creado con éxito...");

			
			// Crea un array de Obras para guardar los libros y hacer las relaciones
			Obra o1[] = new Obra[2];
			o1[0] = l1;
			o1[1] = l2;

			// Guarda las relaciones del autor con los libros que ha escrito.
			autor.setEscribe(o1);

			// Ejecuta un commit para materializar las peticiones.
			db.commit();

			// Cierra la base de datos.
			db.close();

			System.out.println("\nCreación del Autor y sus Obras escritas con éxito.");

		} catch (MtException mte) {
			System.out.println("MtException : " + mte.getMessage());
		}
	}


	// Borrar todos los objetos de una clase
	public static void borrarTodos(String hostname, String dbname) {
		System.out.println("====================== Borrar Todos=====================\n");
		try {
			MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
			db.open();
			db.startTransaction();

			// Lista todos los objetos Obra que hay en la base de datos, con el método getInstanceNumber
			System.out.println("\n" + Obra.getInstanceNumber(db) + "Obra(s) en la DB.");// Borra todas las instancias de Obra
			Obra.getClass(db).removeAllInstances();


			// Materializa los cambios y cierra la BD
			db.commit();
			db.close();


			System.out.println("\nObras borrradas con éxito.");


		} catch (MtException mte) {
			System.out.println("MtException : " + mte.getMessage());
		}
	}

	public static void modificaObjeto(String hostname, String dbname, String nombre, Integer nuevaEdad) {

		System.out.println("=========== Modifica un objeto==========\n");
		int nAutores = 0;

		try 
		{
			MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
			db.open();
			db.startTransaction();

			// Lista cuántos objetos Autores con el métodogetInstanceNumber
			System.out.println("\n" + Autor.getInstanceNumber(db) + "Autores en la DB.");

			nAutores = (int) Autor.getInstanceNumber(db);


			// Crea un Iterador (propio de Java)
			MtObjectIterator<Autor> iter =Autor.<Autor>instanceIterator(db);
		

			while (iter.hasNext()) 
			{
				Autor[] autores = iter.next(nAutores);
				for (int i = 0; i < autores.length; i++) 
				{

					// Busca una autor con nombre 'nombre'
					if (autores[i].getNombre().compareTo(nombre)== 0) 
					{
						autores[i].setEdad(nuevaEdad);	
					}
				}
			}
			iter.close();

			// materializa los cambios y cierra la BD
			db.commit();
			db.close();
			System.out.println("\nAutor modificado con éxito.");

		} catch (MtException mte) {
			System.out.println("MtException : " + mte.getMessage());
		}
	}

	public static void ejecutaOQL(String hostname, String dbname) {
		
		MtDatabase dbcon = new MtDatabase(hostname, dbname);

		// Abre una conexión a la base de datos
		dbcon.open();
		try {
			
			// Crea una instancia de Statement
			Statement stmt = dbcon.createStatement();
			// Asigna una consulta OQL. Esta consulta lo que hace es utilizar REF() para
			// obtener el objeto
			// directamente en vez de obtener valores concretos (que también podría ser).
			String commandText = "SELECT REF(a) from biblioteca.Autor a;";
			
			// Ejecuta la consulta y obtiene un ResultSet
			ResultSet rset = stmt.executeQuery(commandText);
			Autor a1;
				
			// Lee rset uno a uno.
			while (rset.next()) 
			{
				// Obtiene los objetos Autor.
				a1 = (Autor) rset.getObject(1);
				// Imprime los atributos de cada objeto con un formato determinado.
				System.out.println("Autor: " + String.format("%16s", a1.getNombre()) + String.format("%16s", a1.getApellidos()) + " Spouse: " + String.format("%16s", a1.getEdad()));
			}
			// Cierra las conexiones
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
		}
	}
}