import org.apache.camel.CamelContext
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.component.sql.SqlComponent
import org.apache.camel.dataformat.csv.CsvDataFormat
import io.github.cdimascio.dotenv.Dotenv
import com.mysql.cj.jdbc.MysqlDataSource
import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

class CsvToDatabaseImporter {
    static void main(String[] args) {
        if (args.length != 1) {
            println "Usage: CsvToDatabaseImporter <csv-file-name>"
            System.exit(1)
        }

        Dotenv dotenv = Dotenv.load()
        String csvFileName = args[0]
        String dbHost = dotenv.get("DB_URL")
        String user = dotenv.get("DB_USER")
        String password = dotenv.get("DB_PASSWORD")

        if (!dbHost || !user || !password) {
            println "Database connection details are not set in environment variables."
            System.exit(1)
        }

        DataSource dataSource = createDataSource(dbHost, user, password)
        if (dataSource == null) {
            println "Failed to create DataSource."
            System.exit(1)
        }

        if (!testDatabaseConnection(dataSource)) {
            println "Failed to connect to the database. Please check your database settings."
            System.exit(1)
        }

        println "DataSource created with URL: jdbc:mysql://${dbHost}, User: ${user}"

        // Print out the tables in the database
        printDatabaseTables(dataSource)

        CamelContext context = new DefaultCamelContext()

        try {
            // Create and configure the SqlComponent
            SqlComponent sqlComponent = new SqlComponent()
            sqlComponent.setDataSource(dataSource)
            context.addComponent("sql", sqlComponent)
            println "SqlComponent created and DataSource assigned."

            // Print SqlComponent details
            printSqlComponentDetails(sqlComponent)

            // Add routes
            context.addRoutes(new RouteBuilder() {
                @Override
                void configure() {
                    CsvDataFormat csv = new CsvDataFormat()
                    csv.setUseMaps(true)

                    from("file:src/csv?fileName=" + csvFileName + "&noop=true")
                        .log("Picked up file: \${header.CamelFileName}")
                        .unmarshal(csv)
                        .log("Unmarshalled CSV data: \${body}")
                        .split().simple("\${body}")
                        .log("Processing record: \${body}")
                        .process(exchange -> {
                            Map<String, Object> data = exchange.getIn().getBody(Map.class)
                            exchange.getIn().setHeader("name", data.get("name"))
                            exchange.getIn().setHeader("age", data.get("age"))
                            exchange.getIn().setHeader("email", data.get("email"))
                        })
                        .to("sql:INSERT INTO users (name, age, email) VALUES (:#name, :#age, :#email)")
                        .log("Inserted record into database")
                        .end()
                        .process(exchange -> {
                            println "Shutting down Camel context..."
                            context.stop()
                            System.exit(0)
                        })
                }
            })

            println "Starting Camel context..."
            context.start()

            // Keep the context running to process the file
            synchronized(context) {
                context.wait()
            }
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            println "Stopping Camel context..."
            try {
                context.stop()
            } catch (Exception e) {
                e.printStackTrace()
            }
        }
    }

    static DataSource createDataSource(String dbHost, String user, String password) {
        try {
            MysqlDataSource dataSource = new MysqlDataSource()
            dataSource.setURL("jdbc:mysql://${dbHost}")
            dataSource.setUser(user)
            dataSource.setPassword(password)
            
            // Print out dataSource properties for verification
            println "DataSource URL: " + dataSource.getURL()
            println "DataSource User: " + dataSource.getUser()
            println "DataSource ServerName: " + dataSource.getServerName()
            println "DataSource Port: " + dataSource.getPort()
            println "DataSource DatabaseName: " + dataSource.getDatabaseName()

            return dataSource
        } catch (Exception e) {
            e.printStackTrace()
            return null
        }
    }

    static boolean testDatabaseConnection(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            if (connection != null && !connection.isClosed()) {
                println "Successfully connected to the database."
                return true
            }
        } catch (Exception e) {
            e.printStackTrace()
        }
        return false
    }

    static void printDatabaseTables(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW TABLES")) {

            println "Tables in the database:"
            while (resultSet.next()) {
                println resultSet.getString(1)
            }
        } catch (Exception e) {
            e.printStackTrace()
        }
    }

    static void printSqlComponentDetails(SqlComponent sqlComponent) {
        println "SqlComponent details:"
        println "DataSource: " + sqlComponent.getDataSource()
        // Printing dataSource properties
        if (sqlComponent.getDataSource() instanceof MysqlDataSource) {
            MysqlDataSource dataSource = (MysqlDataSource) sqlComponent.getDataSource()
            println "SqlComponent DataSource URL: " + dataSource.getURL()
            println "SqlComponent DataSource User: " + dataSource.getUser()
            println "SqlComponent DataSource ServerName: " + dataSource.getServerName()
            println "SqlComponent DataSource Port: " + dataSource.getPort()
            println "SqlComponent DataSource DatabaseName: " + dataSource.getDatabaseName()
        }
    }
}
