package com.chen;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class test {
    public static void main(String[] args) throws IOException {
        //原始构建
        //数据预处理 按照基数排序
        Build.dataPreProcessing();
        long startbuild=System.nanoTime();
        Build.buildIndex();
        long endbuild=System.nanoTime();
        long buildtime=(endbuild-startbuild)/1_000_000_000;
        System.out.println("首次构建的时间"+buildtime+"s");

        Build.serializeAll();

        //反序列化构建
//        long startbuild=System.nanoTime();
//        Build.buildFromSER();
//        long endbuild=System.nanoTime();
//        long buildtime=(endbuild-startbuild)/1_000_000_000;
//        System.out.println("反序列化构建的时间"+buildtime+"s");

        //按列查询
//        long startquery=System.nanoTime();
//        Query.queryFile("D:\\Code\\Idea_Codes\\HIBF_FILE\\query.txt");
//        long endquery=System.nanoTime();
//        long querytime=(endquery-startquery)/1_000_000;
//        System.out.println("查询时间"+querytime+"ms");


        //按行查询
        HIBF.transposeTOrow();
        long startquery2=System.nanoTime();
        Query.queryFileASrow("D:\\Code\\Idea_Codes\\HIBF_FILE\\query.txt");
        long endquery2=System.nanoTime();
        long querytime2=(endquery2-startquery2)/1_000_000;
        System.out.println("查询时间"+querytime2+"ms");
        System.out.println("直接计算查询时间"+Query.getQuery_time()+"ms");
        System.out.println("平均每个Kmer查询的布隆过滤器数量:"+Query.average_query_bf_count());

    }
}
