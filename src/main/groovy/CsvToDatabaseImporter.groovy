import org.apache.camel.CamelContext
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.component.jdbc.JdbcComponent
import groovy.sql.Sql
import io.github.cdimascio.dotenv.Dotenv
import javax.sql.DataSource
import com.mysql.cj.jdbc.MysqlDataSource
import com.opencsv.CSVReader
import java.io.FileReader
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.InvalidPathException

class CsvToDatabaseImporter {

    static void main(String[] args) {
        if (args.length != 1) {
            println "Usage: CsvToDatabaseImporter <csv-file-path>"
            System.exit(1)
        }

        Dotenv dotenv = Dotenv.load()
        String csvFilePath = args[0]
        String jdbcUrl = dotenv.get("DB_URL")
        String user = dotenv.get("DB_USER")
        String password = dotenv.get("DB_PASSWORD")

        if (!jdbcUrl || !user || !password) {
            println "Database connection details are not set in environment variables."
            System.exit(1)
        }

        DataSource dataSource = createDataSource(jdbcUrl, user, password)
        Sql sql = new Sql(dataSource)

        try {
            List<Map<String, String>> csvData = readCsvFile(csvFilePath)
            importCsvData(sql, csvData)
            println "CSV data imported successfully."
        } catch (Exception e) {
            println "Error: " + e.message
            e.printStackTrace()
            System.exit(1)
        } finally {
            sql.close()
        }
    }

    static DataSource createDataSource(String jdbcUrl, String user, String password) {
        MysqlDataSource dataSource = new MysqlDataSource()
        dataSource.setURL(jdbcUrl)
        dataSource.setUser(user)
        dataSource.setPassword(password)
        return dataSource
    }

    static List<Map<String, String>> readCsvFile(String csvFilePath) {
        // Define the allowed directory
        String allowedDirectory = "src/csv"

        // Validate and sanitize the file path
        Path filePath
        try {
            filePath = Paths.get(csvFilePath).normalize()
            if (!filePath.startsWith(Paths.get(allowedDirectory))) {
                throw new RuntimeException("Invalid file path: Access outside of allowed directory is not permitted")
            }
        } catch (InvalidPathException e) {
            throw new RuntimeException("Invalid file path: " + e.message)
        }

        CSVReader reader = new CSVReader(new FileReader(filePath.toFile()))
        List<Map<String, String>> csvData = []
        String[] headers = reader.readNext()
        String[] line
        while ((line = reader.readNext()) != null) {
            Map<String, String> row = [:]
            for (int i = 0; i < headers.length; i++) {
                row[headers[i]] = line[i]
            }
            csvData.add(row)
        }
        reader.close()
        return csvData
    }

    static void importCsvData(Sql sql, List<Map<String, String>> csvData) {
        csvData.each { row ->
            sql.executeInsert("INSERT INTO users (name, age, email) VALUES (:name, :age, :email)", row)
        }
    }
}