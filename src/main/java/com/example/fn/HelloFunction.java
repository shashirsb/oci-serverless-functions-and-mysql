package com.example.fn;

import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResult;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.StreamSummary;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;

import static java.nio.charset.StandardCharsets.UTF_8;

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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.io.*;
import java.net.*;

public class HelloFunction {

    private StreamAdminClient sAdminClient = null;
    private StreamClient streamClient = null;
    final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
            .builder().build();

    public String handleRequest(String input) throws SQLException, URISyntaxException {


        try {

      
            sAdminClient = new StreamAdminClient(provider);
      
            String region = System.getenv().get("STREAM_REGION"); // e.g. us-phoenix-1
       
            sAdminClient.setEndpoint("https://cell-1.streaming.us-ashburn-1.oci.oraclecloud.com");
         
            produce();
          
        } catch (Throwable ex) {
            System.err.println("Error occurred in StreamProducerFunction constructor - " + ex.getMessage());
        }

        // Insert into MYSQL
        String name = (input == null || input.isEmpty()) ? "tamo-iot" : input;

        URI uri = new URI("file:///tmp/iot.csv");
        // File homedir = new File(System.getProperty("user.home"));
        File file = new File(uri);
        String csvFileUrl = "https://objectstorage.us-ashburn-1.oraclecloud.com/p/n5odYj5P7tXVIb3X13wUamCIU0-BtiMif9rT-stBk_LEzp93xxgwFziQEF2cAP0u/n/sehubjapacprod/b/tamo-input-iot-files/o/people.csv";

        try (BufferedInputStream in = new BufferedInputStream(new URL(csvFileUrl).openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(file)) {

            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);

            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);
        }

        Configuration configuration = initMybatis();
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        try (SqlSession session = sqlSessionFactory.openSession()) {
            PersonMapper personMapper = session.getMapper(PersonMapper.class);

            // int batchSize = 20;
            BufferedReader lineReader = new BufferedReader(new FileReader(file));
            String lineText = null;

            lineReader.readLine(); // skip header line
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                Person newPerson = new Person();
                newPerson.setId(Long.parseLong(data[0]));
                newPerson.setFirstName(data[1]);
                newPerson.setLastName(data[2]);
                
            }

            // List<Person> persons = personMapper.selectAll();
            //session.commit();
            session.close();
            lineReader.close();
        } catch (FileNotFoundException e) {
            System.out.println(e);
        } catch (IOException ex) {
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

    public String produce() {
        String result = null;

        if (sAdminClient == null) {
            result = "Stream Admin client is not ready. Please check for errors in constructor";
            System.out.println(result);
            return result;
        }

        try {

            ListStreamsRequest listStreamsRequest = ListStreamsRequest.builder()
                    .name("iotdata")
                    .compartmentId(
                            "ocid1.compartment.oc1..aaaaaaaah6ibn4qjy6chh7ilzha53oeeacmrmghdh5ziqhzn2xtgubhxolga")
                    .build();

            System.out.println("listing streams in compartment "
                    + "ocid1.compartment.oc1..aaaaaaaah6ibn4qjy6chh7ilzha53oeeacmrmghdh5ziqhzn2xtgubhxolga");
            ListStreamsResponse listStreamsResponse = sAdminClient.listStreams(listStreamsRequest);
            List<StreamSummary> streams = listStreamsResponse.getItems();

            if (streams.isEmpty()) {
                String errMsg = "Stream with name " + "iotdata" + " not found in compartment "
                        + "ocid1.compartment.oc1..aaaaaaaah6ibn4qjy6chh7ilzha53oeeacmrmghdh5ziqhzn2xtgubhxolga";
                System.out.println(errMsg);
                return errMsg;
            }

            String streamOCID = streams.get(0).getId();
            System.out.println("Found stream with OCID -- " + streamOCID);

            String streamClientEndpoint = streams.get(0).getMessagesEndpoint();
            System.out.println("Stream client endpoint " + streamClientEndpoint);

            streamClient = new StreamClient(provider);
            streamClient.setEndpoint(streamClientEndpoint);

            // PutMessagesDetails putMessagesDetails = PutMessagesDetails.builder()
            // .messages(Arrays.asList(PutMessagesDetailsEntry.builder().key("hello".getBytes())
            // .value("world".getBytes()).build()))
            // .build();

            // PutMessagesRequest putMessagesRequest = PutMessagesRequest.builder()
            // .putMessagesDetails(putMessagesDetails)
            // .streamId(streamOCID)
            // .build();

            // PutMessagesResult putMessagesResult =
            // streamClient.putMessages(putMessagesRequest).getPutMessagesResult();
            // System.out.println("pushed messages...");

            // for (PutMessagesResultEntry entry : putMessagesResult.getEntries()) {
            // if (entry.getError() != null) {
            // result = "Put message error " + entry.getErrorMessage();
            // System.out.println(result);
            // } else {
            // result = "Message pushed to offset " + entry.getOffset() + " in partition " +
            // entry.getPartition();
            // System.out.println(result);
            // }
            // }

            // A cursor can be created as part of a consumer group.
            // Committed offsets are managed for the group, and partitions
            // are dynamically balanced amongst consumers in the group.
            System.out.println("Starting a simple message loop with a group cursor");
            String groupCursor = getCursorByGroup(streamClient, "ocid1.stream.oc1.iad.amaaaaaaak7gbriastx37iu27vegisyx33smp36ijherxrmeota5c3w4u77a", "exampleGroup", "exampleInstance-1");
            simpleMessageLoop(streamClient, "ocid1.stream.oc1.iad.amaaaaaaak7gbriastx37iu27vegisyx33smp36ijherxrmeota5c3w4u77a", groupCursor);

        } catch (Exception e) {
            result = "Error occurred - " + e.getMessage();

            System.out.println(result);
        }
        /*
         * finally {
         * sAdminClient.close();
         * streamClient.close();
         * System.out.println("Closed stream clients");
         * }
         */

        return result;
    }

    private static void simpleMessageLoop(
            StreamClient streamClient, String streamId, String initialCursor) {
        String cursor = initialCursor;
        for (int i = 0; i < 10; i++) {

            GetMessagesRequest getRequest = GetMessagesRequest.builder()
                    .streamId(streamId)
                    .cursor(cursor)
                    .limit(25)
                    .build();

            GetMessagesResponse getResponse = streamClient.getMessages(getRequest);

            // process the messages
            System.out.println(String.format("Read %s messages.", getResponse.getItems().size()));
            for (Message message : ((GetMessagesResponse) getResponse).getItems()) {
                System.out.println(
                        String.format(
                                "%s: %s",
                                message.getKey() == null ? "data" : new String(message.getKey(), UTF_8),
                                new String(message.getValue(), UTF_8)));
            }

            // getMessages is a throttled method; clients should retrieve sufficiently large
            // message
            // batches, as to avoid too many http requests.
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

            // use the next-cursor for iteration
            cursor = getResponse.getOpcNextCursor();
        }
    }

    private static String getCursorByGroup(
            StreamClient streamClient, String streamId, String groupName, String instanceName) {
        System.out.println(
                String.format(
                        "Creating a cursor for group %s, instance %s.", groupName, instanceName));

        CreateGroupCursorDetails cursorDetails = CreateGroupCursorDetails.builder()
                .groupName(groupName)
                .instanceName(instanceName)
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .commitOnGet(true)
                .build();

        CreateGroupCursorRequest createCursorRequest = CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .createGroupCursorDetails(cursorDetails)
                .build();

        CreateGroupCursorResponse groupCursorResponse = streamClient.createGroupCursor(createCursorRequest);
        return groupCursorResponse.getCursor().getValue();
    }
}
