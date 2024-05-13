package com.chen;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Query {
    //按列查询
    public static List<String> querykmer(String kmer){
        Queue<Integer> to_check_ibf = new LinkedList<>();
        List<String> result_samples=new ArrayList<>();//结果数据集名称
        //一层一层查 首先查第一层索引为0的IBF
        to_check_ibf.add(0);
        while (!to_check_ibf.isEmpty()){
            int ibf_index=to_check_ibf.peek();//队头元素
//            System.out.println("当前查询IBF索引："+ibf_index);
            IBF query_ibf=HIBF.getIbfByIndex(ibf_index);//当前要检查的ibf
            List<Integer> contain_bins=new ArrayList<>();//记录当前查询的ibf中包含查询元素的bin的索引
            contain_bins=query_ibf.queryAScloum(kmer);
//            System.out.println("当前ibf包含查询元素的bin索引"+contain_bins);
            //如果是split bin和single bin，直接可以获得查询到的数据集
            for(int bin_index:contain_bins){
                //获取这个bin
                TechnicalBin report_bin=query_ibf.getBinByindex(bin_index);
                if(report_bin.getBin_type().equals("split")||report_bin.getBin_type().equals("single")){
                    int dataset_index=report_bin.getDatasets().get(0);
                    //获得数据集名称
                    String dataset_name=Metadata.getNameByIdx(dataset_index);
                    result_samples.add(dataset_name);
                }else{//是merge bin，需要继续向下查
                    //要继续查它的孩子节点
                    int child_ibf=report_bin.getChild_ibf();
                    to_check_ibf.add(child_ibf);
                }
            }
            to_check_ibf.poll();
        }
        return result_samples;
    }

    public static void querySequence(String sequence, BufferedWriter writer) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> result=new ArrayList<>(querykmer(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmer(kmer));
        }

        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    public static void queryFile(String filePath){//一个文件中有多个查询长序列，查询每一个并把查询结果写入输出文件
        String queryresultFile = "D:/Code/Idea_Codes/HIBF_FILE"+"/"+"HIBF_query_result(col).txt";//存放查询结果
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        querySequence(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                querySequence(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }

    //按行查询
    public static List<String> querykmerASrow(String kmer){
        Queue<Integer> to_check_ibf = new LinkedList<>();
        List<String> result_samples=new ArrayList<>();//结果数据集名称
        //一层一层查 首先查第一层索引为0的IBF
        to_check_ibf.add(0);
        while (!to_check_ibf.isEmpty()){
            int ibf_index=to_check_ibf.peek();//队头元素
//            System.out.println("当前查询IBF索引："+ibf_index);
            IBF query_ibf=HIBF.getIbfByIndex(ibf_index);//当前要检查的ibf
            List<Integer> contain_bins=new ArrayList<>();//记录当前查询的ibf中包含查询元素的bin的索引
            //按行查询
            contain_bins=query_ibf.queryASrow(kmer);
//            System.out.println("当前ibf包含查询元素的bin索引"+contain_bins);
            //如果是split bin和single bin，直接可以获得查询到的数据集
            for(int bin_index:contain_bins){
                //获取这个bin
                TechnicalBin report_bin=query_ibf.getBinByindex(bin_index);
                if(report_bin.getBin_type().equals("split")||report_bin.getBin_type().equals("single")){
                    int dataset_index=report_bin.getDatasets().get(0);
                    //获得数据集名称
                    String dataset_name=Metadata.getNameByIdx(dataset_index);
                    result_samples.add(dataset_name);
                }else{//是merge bin，需要继续向下查
                    //要继续查它的孩子节点
                    int child_ibf=report_bin.getChild_ibf();
                    to_check_ibf.add(child_ibf);
                }
            }
            to_check_ibf.poll();
        }
        return result_samples;
    }
    public static void querySequenceASrow(String sequence, BufferedWriter writer) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> result=new ArrayList<>(querykmerASrow(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmerASrow(kmer));
        }

        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    public static void queryFileASrow(String filePath){//一个文件中有多个查询长序列，查询每一个并把查询结果写入输出文件
        String queryresultFile = "D:/Code/Idea_Codes/HIBF_FILE"+"/"+"HIBF_query_result(row).txt";//存放查询结果
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        querySequenceASrow(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                querySequenceASrow(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }

}
