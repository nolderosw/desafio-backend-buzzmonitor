import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class ElasticSearch extends Json {

    public void fillDataOnDB(String url, TransportClient client) throws IOException{
        JSONArray data = readJsonFromUrl(url).getJSONArray("data");
        for (int i = 0; i < data.length(); i++){
            client.prepareIndex("bm-posts-saas-2019-5", "posts", data.getJSONObject(i)
                    .get("elasticsearch_id").toString())
                    .setSource(data.getJSONObject(i).toString(), XContentType.JSON)
                    .get();
        }
    }
    public void getPostsFromTerm (String term, TransportClient client){
        SearchResponse response = client.prepareSearch("bm-posts-saas-2019-5")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000)
                .setFetchSource(new String[]{"hashtags"},null)
                .setPostFilter(QueryBuilders.multiMatchQuery(term,"content","title"))
                .execute()
                .actionGet();

        SearchHit[] result = response.getHits().getHits();
        JSONArray results = new JSONArray();
        for (int i = 0; i < result.length; i++) {
            results.put(i, new JSONObject(result[i]).get("sourceAsMap"));
        }
        formatOutput(getTop10HashTags(results));

    }
    private static Map<String, Integer> getTop10HashTags (JSONArray results){
        HashMap<String, Integer> hashtags = new HashMap<String, Integer>();
        for (int i = 0; i < results.length(); i++){
            if(results.getJSONObject(i).has("hashtags")){
                List<String> tempValues = Arrays.asList(results.getJSONObject(i).get("hashtags").
                        toString().replace("[", "").replace("]", "")
                        .split(","));
                for (String hash : tempValues) {
                    if(hashtags.containsKey(hash)){
                        int tempValue = hashtags.get(hash) + 1;
                        hashtags.put(hash,tempValue);
                    }
                    else{
                        hashtags.put(hash,1);
                    }
                }
            }
        }
        Map<String, Integer> sortedHashtags = hashtags.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
        return sortedHashtags;
    }
    private void formatOutput (Map<String, Integer> sortedHashtags){
        JSONObject jsonOutput = new JSONObject();
        List<JSONObject> results = new ArrayList();
        Set<String> hashtags = sortedHashtags.keySet();
        for (String hashtag : hashtags){
            JSONObject tempJsonHashtags = new JSONObject();
            tempJsonHashtags.put("name",hashtag.replaceAll("\\\"",""));
            tempJsonHashtags.put("value",sortedHashtags.get(hashtag));
            results.add(tempJsonHashtags);
        }
        jsonOutput.put("results",results);
        System.out.println(jsonOutput);
    }
}
