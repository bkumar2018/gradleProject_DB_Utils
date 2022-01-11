package util;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchDBUtils {

    Settings settings;
    TransportClient client;

    public ElasticSearchDBUtils(String host) {

        settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
        } catch (final UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public TransportClient getClient() {
        return client;
    }

    public SearchResponse getAllDetails(BoolQueryBuilder bool, String reqFields[], String index, String type, String fileName) {

        final SearchRequestBuilder srb = client.prepareSearch(index).setTypes("region").setQuery(bool);
        srb.setFetchSource(reqFields, null);
        srb.setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH);
        final SearchResponse response = srb.setSize(10000).execute().actionGet();
        return response;
    }

    public SearchResponse getAllDetails(BoolQueryBuilder bool, String index) {
        final SearchRequestBuilder srb = client.prepareSearch(index.split(",")).setQuery(bool).setSize(10000);
        srb.setSearchType(SearchType.QUERY_AND_FETCH);
        final SearchResponse response = srb.execute().actionGet();
        return response;
    }

    public boolean isIndexExists(String index) {
        boolean exists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
        return exists;
    }

    public SearchResponse getAllDetails(BoolQueryBuilder bool, String index, SortBuilder sort, int size) {

        final SearchRequestBuilder srb = client.prepareSearch(index.split(",")).addSort(sort).setQuery(bool).setSize(10000);
        srb.setSearchType(SearchType.QUERY_AND_FETCH);
        final SearchResponse response = srb.execute().actionGet();

        return  response;
    }

    public SearchResponse getAllDetails(BoolQueryBuilder bool, String reqFields[], String index, String type) {

        final SearchRequestBuilder srb = client.prepareSearch(index).setTypes("region").setQuery(bool);
        srb.storedFields("*");
        srb.setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH);
        final SearchResponse response = srb.setSize(10000).execute().actionGet();
        return response;
    }

    public SearchResponse getAllDetails(BoolQueryBuilder bool, String index, SortBuilder sort) {

        final SearchRequestBuilder srb = client.prepareSearch(index.split(",")).addSort(sort).setQuery(bool);
        srb.setSearchType(SearchType.QUERY_AND_FETCH);
        final SearchResponse response = srb.execute().actionGet();
        return response;
    }

}
