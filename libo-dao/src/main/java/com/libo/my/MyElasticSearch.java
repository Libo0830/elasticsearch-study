package com.libo.my;

import net.sf.json.JSONArray;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @FileName: MyElasticSearch
 * @author: bli
 * @date: 2020年02月20日 9:39
 * @description:
 */
public class MyElasticSearch {

    private static RestHighLevelClient client = null;

    public MyElasticSearch(){
        client = MyElasticSearchClient.getRestHighLevelClient();
    }

    /**
     * 新增
     */
    public static void index(List list, Class<?> cls) {
        //获取类名并将首字母转小写
        String className = lowerFirstChar(cls.getSimpleName());
        if (list != null && !list.isEmpty()){
            list.forEach(item->{
                try {
                    Map<String, Object> paramJsonMap = BeanUtils.describe(item);
                    //去掉转换后map中的class  key
                    paramJsonMap.remove("class");
                    IndexRequest request = new IndexRequest(className, className,paramJsonMap.get("id").toString()).source(paramJsonMap);
                    //同步执行
                    IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                    String index = indexResponse.getIndex();
                    String id = indexResponse.getId();
                    DocWriteResponse.Result result = indexResponse.getResult();
                    System.out.println("index:"+ index+",id:"+id+",result:"+result);
                    if (result == DocWriteResponse.Result.CREATED) {
                        //处理(如果需要)第一次创建文档的情况
                        System.out.println("文件创建成功");
                    } else if (result == DocWriteResponse.Result.UPDATED) {
                        //处理(如果需要的话)当文档已经存在时被重写的情况
                        System.out.println("文件更新成功");
                    }
                    ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                    if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                        //处理成功碎片的数量少于总碎片的情况
                        System.out.println("处理成功碎片的数量少于总碎片");
                    }
                    if (shardInfo.getFailed() > 0) {
                        for (ReplicationResponse.ShardInfo.Failure failure :
                                shardInfo.getFailures()) {
                            //处理潜在的故障
                            String reason = failure.reason();
                            System.out.println("故障："+ reason);
                        }
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
        }


    }

    /**
     * 批量新增
     */
    public static void bulkCreateIndex(List list, Class<?> cls) throws IOException {
        //获取类名并将首字母转小写
        String className = lowerFirstChar(cls.getSimpleName());
        BulkRequest bulkRequest = new BulkRequest();
        if (list != null && !list.isEmpty()){
            list.forEach(item->{
                try {
                    Map<String, Object> paramJsonMap = BeanUtils.describe(item);
                    //去掉转换后map中的class  key
                    paramJsonMap.remove("class");
                    bulkRequest.add(new IndexRequest(className, className,paramJsonMap.get("id").toString()).source(paramJsonMap));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }

            });
            //同步执行
            BulkResponse bulkResponse = client.bulk(bulkRequest);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    continue;
                }
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                }
            }

        }
    }


    /**
     * 查询
     * @throws IOException
     */
    public static List search(String queryColum, String param, Integer pageIndex, Integer pageSize, Class<?> cls) throws IOException, InvocationTargetException, IllegalAccessException, InstantiationException {
        //获取类名并将首字母转小写
        String className = lowerFirstChar(cls.getSimpleName());
        //创建SearchRequest，其中构造参数为索引名称，type为类型名称，此处建议创建ES索引时，索引名称和类型名称相同。
        SearchRequest searchRequest = new SearchRequest(className).types(className);
        //使用默认选项创建一个SearchSourceBuilder。
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置查询。可以是任何类型的QueryBuilder，此处因为要使用分词器，需要使用match查询，分词器是和math匹配的。
        sourceBuilder.query(QueryBuilders.matchQuery(queryColum, param));
        //设置分页
        sourceBuilder.from(pageIndex*pageSize);
        sourceBuilder.size(pageSize);
        //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //将SearchSourceBuilder添加到SearchRequest中
        searchRequest.source(sourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        List list = new ArrayList();
        for (SearchHit hit : searchHits) {
            Object obj = cls.newInstance();
            //转换查询结果，mapToBean
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            BeanUtils.populate(obj, sourceAsMap);
            System.out.println(sourceAsMap);
            list.add(obj);
        }
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(list);
        System.out.println(jsonArray.toString());
        return list;
    }



    /**
     * 将类名首字母小写
     * @param str
     * @return
     */
    private static String lowerFirstChar(String str){
        char [] chars = str.toCharArray();
        chars[0] += 32;
        return String.valueOf(chars);
    }
}
