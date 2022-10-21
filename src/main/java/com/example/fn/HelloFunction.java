package com.example.fn;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.TypeAliasRegistry;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

import java.io.*;
import java.net.*;

public class HelloFunction {

    public String handleRequest(String input) throws SQLException, URISyntaxException {
        String name = (input == null || input.isEmpty()) ? "tamo-iot" : input;

      
        URI uri;

        uri = new URI("file:///tmp/iot1.csv");
        // File homedir = new File(System.getProperty("user.home"));
        File file = new File(uri);
        String csvFileUrl = "https://objectstorage.us-ashburn-1.oraclecloud.com/p/n5odYj5P7tXVIb3X13wUamCIU0-BtiMif9rT-stBk_LEzp93xxgwFziQEF2cAP0u/n/sehubjapacprod/b/tamo-input-iot-files/o/people.csv";

        System.out.println("----------------------------1");

        try (BufferedInputStream in = new BufferedInputStream(new URL(csvFileUrl).openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            System.out.println("----------------------------2a");
            // PrintWriter writer = new PrintWriter(file, "UTF-8");
            // writer.println("The first line");
            // writer.println("13,shail,kumar");
            // writer.close();

            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            System.out.println("----------------------------2b");
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
                System.out.println(dataBuffer);
            }
            System.out.println("----------------------------3");
        } catch (FileNotFoundException e) {
            System.out.println("----------------------------3a");
            System.out.println(e);
        } catch (IOException e) {
            System.out.println("----------------------------3b");
            System.out.println(e);
        }

        System.out.println("----------------------------4");
        Configuration configuration = initMybatis();
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        try (SqlSession session = sqlSessionFactory.openSession()) {
            PersonMapper personMapper = session.getMapper(PersonMapper.class);

            System.out.println("----------------------------5");
            int batchSize = 20;
            BufferedReader lineReader = new BufferedReader(new FileReader(file));
            String lineText = null;
            int count = 0;
            lineReader.readLine(); // skip header line
            System.out.println("----------------------------6");
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                Person newPerson = new Person();
                newPerson.setId(Long.parseLong(data[0]));
                newPerson.setFirstName(data[1]);
                newPerson.setLastName(data[2]);
                Integer insertCount = personMapper.insert(newPerson);
                System.out.println(insertCount);
                System.out.println("----------------------------7");
            }

            System.out.println("----------------------------8");
            List<Person> persons = personMapper.selectAll();
            for (Person person : persons) {
                System.out.println(person);
            }
            System.out.println("----------------------------9");
            session.commit();
        } catch (FileNotFoundException e) {
            System.out.println("----------------------------9a");
            System.out.println(e);
        } catch (IOException ex) {
            System.out.println("----------------------------9b");
            System.err.println(ex);
        }

        System.out.println("Inside Java Hello World function");
        return "Hello, " + name + "!";
    }

    Configuration initMybatis() throws SQLException {
        DataSource dataSource = getDataSource();
        TransactionFactory trxFactory = new JdbcTransactionFactory();

        Environment env = new Environment("dev", trxFactory, dataSource);
        Configuration config = new Configuration(env);
        TypeAliasRegistry aliases = config.getTypeAliasRegistry();
        aliases.registerAlias("person", Person.class);

        config.addMapper(PersonMapper.class);
        return config;
    }

    DataSource getDataSource() throws SQLException {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setDatabaseName("iotdb");
        dataSource.setServerName("1.1.0.214");
        dataSource.setPort(3306);
        dataSource.setUser("admin");
        dataSource.setPassword("MyDB@12345678");
        dataSource.setServerTimezone("UTC");

        return dataSource;
    }

}
