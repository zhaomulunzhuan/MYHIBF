package com.chen;
import net.jpountz.xxhash.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.datasketches.hll.HllSketch;



public class HyperLogLog {


    public static void main(String[] args) throws IOException {
        HyperLogLog_estimates();
        Metadata.sortCardinality();


    }
    public static void HyperLogLog_estimates() throws IOException {
        String filePath = "D:\\Code\\Idea_Codes\\HIBF_FILE\\HIBF_inputfiles.txt"; // 输入文件路径
        List<String> filePaths = readFilePaths(filePath);

        for (String filepath : filePaths) {
            Metadata.addDataset(filepath);

            String fileName = filepath.substring(filepath.lastIndexOf("\\") + 1);//数据集名
            int datasetIndex= Metadata.getIdxByName(fileName);//数据集索引

            List<String> kmers = readKmersFromFile(filepath);

            int estimatecardinality = HLLcalculate(kmers);
            Metadata.addcardinality(datasetIndex,estimatecardinality);

//            int exactcardinality= calculateExactKmerCardinality(kmers);//真实值
//            System.out.println(fileName+":"+estimatecardinality+" "+exactcardinality+" "+(Math.abs(estimatecardinality-exactcardinality)/exactcardinality)*100);
        }
    }

    // 从文件中读取文件路径并返回列表
    public static List<String> readFilePaths(String filePath) {
        List<String> fileNames = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                fileNames.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileNames;
    }

    // 从文件中读取 k-mer 并返回列表
    public static List<String> readKmersFromFile(String fileName) {
        List<String> kmers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                kmers.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kmers;
    }

    // 计算给定 k-mer 的 HLL Sketch
    public static int HLLcalculate(List<String> kmers) throws IOException {
        HllSketch sketch = new HllSketch(12); // 12 is the log2m value, adjust based on your requirements
        //log2m 决定了 HLL 中存储桶（buckets）的数量，桶的数量为 2 的 log2m 次方
        //更大的 log2m 值通常意味着更大的内存开销和更高的精度
        String line;
        for(String kmer:kmers) {
            sketch.update(kmer.getBytes());
        }
        return (int) sketch.getEstimate();
    }

    public static int calculateExactKmerCardinality(List<String> kmers) {
        Set<String> kmerSet = new HashSet<>();
        for(String kmer:kmers) {
            kmerSet.add(kmer);
        }
        return kmerSet.size();
    }
}

