package syed.baqir.naqvi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

//import syed.baqar.naqvi.StudentFileData;

public class Studentbatchprocess {
	// JDBC driver name and database URL

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	// static final String DB_URL = "jdbc:mysql://localhost";

	static final String DB_URL = "jdbc:mysql://localhost/STUDENTS";
	static final String FILE_PATH = "C:\\workspace\\Data";
	static final String FILE_NAME = "StdData.txt";

	// Database credentials
		static final String USER = "SyedBaqir";
		static final String PASS = "SyedBaqir123";

	public static void main(String[] args) {

		Connection dbconn = null;
		boolean flag = false;
		try {
			// STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			// STEP 3: Open a connection without a database name to create a new database.
			// It will connect to database system with Admin permissions
			System.out.println("Connecting to database...");
			dbconn = DriverManager.getConnection(DB_URL, USER, PASS);// DB_URL = "jdbc:mysql://localhost"
			Path filePath = Paths.get(FILE_PATH, FILE_NAME);
			Path backfilepath = Paths.get(FILE_PATH + "\\StdDataBackup.txt");
		    Path originalPath = filePath;
		    Files.copy(originalPath, backfilepath, StandardCopyOption.REPLACE_EXISTING);
			if(saveStudentData(dbconn, filePath)== true) {
				Files.deleteIfExists(filePath);
			}

			dbconn.close();

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} // end try

		System.out.println("Goodbye!");

	}

	public static boolean saveStudentData(Connection dbconn, Path fullFilePath) throws SQLException, IOException {

		int counter = 0;
		int batchSize = 3;
		Statement stmt = null;
		stmt = dbconn.createStatement();
		dbconn.setAutoCommit(false);
		String sql = null;
		String name;
		float score;
		int attendance;
		String grade;
		List<String> lines = Files.readAllLines(fullFilePath);
		
		for (String line : lines) {
			
			String[] split = line.split(" ");
			name = split[0];
			score = Float.parseFloat(split[1]);
			grade = split[2];
			attendance = Integer.parseInt(split[3]);
			///////////////////////////////////////////////////////////////////////////////////////

			// Statement stmt = null;
			try {
				sql = "INSERT INTO student (name,score,grade,attendance) " + "VALUES (" + "'" + name + "'" + ", "
						+ score + "," + "'" + grade + "'" + "," + attendance + ")";
				stmt.addBatch(sql);
				counter++;
				if (counter % batchSize == 0) {
					System.out.println("Commit the batch");
					int[] result = stmt.executeBatch();

					dbconn.commit();
					stmt.clearBatch();
				}
				else if(counter == lines.size()) {
					System.out.println("Commit the batch");
					int[] result = stmt.executeBatch();

					dbconn.commit();
					stmt.clearBatch();
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
			} // end try
		}//for loop ends
		

		System.out.println("Save Student Data mehtod");
		return true;
	}
}
