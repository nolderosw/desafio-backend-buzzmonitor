import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.util.Scanner;

public class App {
    static final String URL_DATA = "https://gist.githubusercontent.com/fabiosl/35e498e5da1cd63c166c789619a0d164"+
            "/raw/b9941a1e6b33f67ac2982ff057d01f748af36d5a/sample.json";

    public static void main(String[] args) throws IOException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        ElasticSearch elasticSearchObj = new ElasticSearch();

        Scanner input = new Scanner(System.in);

        //elasticSearchObj.fillDataOnDB(URL_DATA,client);
        // preenche o BD com os dados do JSON disponibilizado para o desafio

        System.out.print("Digite um tema a ser buscado na base: ");
        String searchTerm = input.nextLine();
        System.out.println("Essas são as principais hashtags relacionadas a \"" + searchTerm + "\"");
        elasticSearchObj.getPostsFromTerm(searchTerm,client);

    }
}
