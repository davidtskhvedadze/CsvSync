import org.apache.camel.CamelContext
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.component.sql.SqlComponent
import org.apache.camel.dataformat.csv.CsvDataFormat
import io.github.cdimascio.dotenv.Dotenv
import com.mysql.cj.jdbc.MysqlDataSource
import javax.sql.DataSource
import java.sql.Connection

class CsvToDatabaseImporter {
    static void main(String[] args) {
        if (args.length != 2) {
            println "Usage: CsvToDatabaseImporter <csv-file-name> <table-name>"
            System.exit(1)
        }

        Dotenv dotenv = Dotenv.load()
        String csvFileName = args[0]
        String tableName = args[1]
        String dbHost = dotenv.get("DB_URL")
        String user = dotenv.get("DB_USER")
        String password = dotenv.get("DB_PASSWORD")

        if (!dbHost || !user || !password) {
            println "Database connection details are not set in environment variables."
            System.exit(1)
        }

        DataSource dataSource = createDataSource(dbHost, user, password)
        if (dataSource == null || !testDatabaseConnection(dataSource)) {
            println "Failed to connect to the database. Please check your database settings."
            System.exit(1)
        }

        CamelContext context = new DefaultCamelContext()
        try {
            SqlComponent sqlComponent = new SqlComponent()
            sqlComponent.setDataSource(dataSource)
            context.addComponent("sql", sqlComponent)

            context.addRoutes(new RouteBuilder() {
                @Override
                void configure() {
                    CsvDataFormat csv = new CsvDataFormat()
                    csv.setUseMaps(true)

                    from("file:src/csv?fileName=" + csvFileName + "&noop=true")
                        .log("Picked up file: \${header.CamelFileName}")
                        .unmarshal(csv)
                        .split().simple("\${body}")
                        .process(exchange -> {
                            Map<String, Object> data = exchange.getIn().getBody(Map.class)
                            String headers = data.keySet().join(", ")
                            String values = data.keySet().collect { ":#${it}" }.join(", ")
                            String sql = "INSERT INTO ${tableName} (${headers}) VALUES (${values})"
                            exchange.getIn().setBody(sql)
                            data.each { key, value ->
                                exchange.getIn().setHeader(key, value)
                            }
                        })
                        .toD("sql:\${body}")
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
}