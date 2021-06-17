package wiki.hadoop.es;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ElasticSearchBulkOperator {

    private static final Log LOG = LogFactory.getLog(ElasticSearchBulkOperator.class);

    private static final int MAX_BULK_COUNT = 10000;

//    private static BulkRequestBuilder bulkRequestBuilder = null;
    private static  BulkRequest bulkRequest = null;



    private static final Lock commitLock = new ReentrantLock();

    private static ScheduledExecutorService scheduledExecutorService = null;
    
    static {
    	init();
    }
    
    public static void init() {
    	  // 初始化  bulkRequestBuilder
//        bulkRequestBuilder = ESClient.client.prepareBulk();
//        bulkRequestBuilder = ESClient.client.prepareBulk();
//
//        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // 初始化线程池大小为1
        scheduledExecutorService = Executors.newScheduledThreadPool(1);

       //创建一个Runnable对象，提交待写入的数据，并使用commitLock锁保证线程安全
        final Runnable beeper = () -> {
            commitLock.lock();
            try {
            	LOG.info("Before submission bulkRequest size : " +bulkRequest.numberOfActions());
            	//提交数据至es
                bulkRequest(0);
                LOG.info("After submission bulkRequest size : " +bulkRequest.numberOfActions());
            } catch (Exception ex) {
                LOG.info(ex.getMessage());
//                System.out.println(ex.getMessage());
            } finally {
                commitLock.unlock();
            }
        };

        //初始化延迟10s执行 runnable方法，后期每隔30s执行一次
        scheduledExecutorService.scheduleAtFixedRate(beeper, 10, 30, TimeUnit.SECONDS);
    }
    
    public static void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }
    private static void bulkRequest(int threshold) throws IOException {
        if (bulkRequest.numberOfActions() > threshold) {
//            BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
            BulkResponse response = ESClient.client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (!response.hasFailures()) {
            	int beforeCount = bulkRequest.numberOfActions();
                bulkRequest = new BulkRequest();
//                bulkRequestBuilder = ESClient.client.prepareBulk();

              LOG.info("提交成功,提交前"+beforeCount+"\t提交后:"+bulkRequest.numberOfActions());
            }else {
//            	LOG.error("异常1,待提交的数量为："+bulkRequestBuilder.numberOfActions());
//            	LOG.error("异常信息:"+bulkItemResponse.buildFailureMessage());
//                LOG.error("异常2,待提交的数量为："+bulkRequestBuilder.numberOfActions());
                LOG.error("异常1,待提交的数量为："+bulkRequest.numberOfActions());
                LOG.error("异常信息:"+response.buildFailureMessage());
                LOG.error("异常2,待提交的数量为："+bulkRequest.numberOfActions());
            }
        }
    }

    /**
     * add update builder to bulk
     * use commitLock to protected bulk as thread-save
     */
    public static void addUpdateBuilderToBulk(IndexRequest indexRequest) {
        commitLock.lock();
        try {
            bulkRequest.add(indexRequest);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" update Bulk index error : " + ex.getMessage() + "\t" +ex.toString());
            
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk
     * use commitLock to protected bulk as thread-save
     *
     */
    public static void addDeleteBuilderToBulk(DeleteRequest deleteRequest) {
        commitLock.lock();
        try {
            bulkRequest.add(deleteRequest);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" delete Bulk " + "gejx_test" + " index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    public void test2() throws IOException {

        //创建bulkrequest对象，整合所有操作
        BulkRequest bulkRequest =new BulkRequest();

           /*
        # 1. 删除5号记录
        # 2. 添加6号记录
        # 3. 修改3号记录 名称为 “三号”
         */
        //添加对应操作
        //1. 删除5号记录
        DeleteRequest deleteRequest=new DeleteRequest("person1","5");
        bulkRequest.add(deleteRequest);

        //2. 添加6号记录
        Map<String, Object> map=new HashMap<>();
        map.put("name","六号");
        IndexRequest indexRequest=new IndexRequest("person1").id("6").source(map);
        bulkRequest.add(indexRequest);
        //3. 修改3号记录 名称为 “三号”
        Map<String, Object> mapUpdate=new HashMap<>();
        mapUpdate.put("name","三号");
        UpdateRequest updateRequest=new UpdateRequest("person1","3").doc(mapUpdate);

        bulkRequest.add(updateRequest);
        //执行批量操作


        BulkResponse response = ESClient.client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());

    }
}