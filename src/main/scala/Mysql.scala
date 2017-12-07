import java.sql.DriverManager
import java.sql.Connection
object Mysql {
        def main(args: Array[String]) {
                val driver = "com.mysql.jdbc.Driver"
                val url = "jdbc:mysql://10.88.1.102/aptwebservice"
                val username = "root"
                val password = "mysqladmin"
                var connection:Connection = null
                try {
                        Class.forName(driver)
                        connection = DriverManager.getConnection(url, username, password)
                        val statement = connection.createStatement()
                        val resultSet = statement.executeQuery("SELECT class, type FROM xdr")
                        while ( resultSet.next() ) {
                                val c = resultSet.getString("class")
                                val t = resultSet.getString("type")
                                println("class, type = " + c + ", " + t)
                        }
                }
                catch {
                        case e => e.printStackTrace
                }
                connection.close()
        }
}
