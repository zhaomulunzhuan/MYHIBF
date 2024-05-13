package com.chen;

import java.io.Serializable;
import java.nio.IntBuffer;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.lucene.util.LongBitSet;

public class IBF implements Serializable {//原则上IBF是多个布隆过滤器（这里可以说是TechnicalBin）按行存储得到的一个bitarray，按行存储是指每个布隆过滤器相同索引bit连续存储，这样一次哈希对应的一行可以以更少的内存访问次数获取
    private static final long serialVersionUID = 1L;
    private int level_index;
    private int ibf_index;
    private List<List<Integer>> datasets;//ibf对应的数据集列表，按构建bin的顺序存储进来，由于一个merge_bin的存储多个数据集，是个数据集列表，所以datasets的每个元素是一个bin的数据集列表
    private int TB_num;//这个ibf中对应的technical bin的数量
    List<TechnicalBin> tbs;//技术bin的列表 构建的时候还是逐个bin构建，即按列构建
    private int parent_ibf;//双亲节点，即IBF A中的一个merge bin B的孩子节点是IBF C，那么C的双亲节点记为A（没怎么用到）
    private LongBitSet ibfASrow;//创造按行查询的存储 这是真正的IBF，即tbs对应的一个按行存储的位数组

    public IBF(int levelIndex,int ibfIndex,int parent_ibf){
        this.level_index=levelIndex;
        this.ibf_index=ibfIndex;
        this.TB_num=0;
        this.datasets=new ArrayList<>();
        this.tbs=new ArrayList<>();
        this.parent_ibf=parent_ibf;
    }
    public void addBins(List<TechnicalBin> bins,List<List<Integer>> datasetlist){//添加bins
        //传入bin列表，以及每个bin存储的数据集列表
        tbs.addAll(bins);
        TB_num+=bins.size();
        datasets.addAll(datasetlist);
    }

    public int getTB_num(){
        return TB_num;
    }

    public List<Integer> getDatasetsByBinindex(int bin_index){//根据bin在IBF内的索引获得对应存储的数据集列表
        return datasets.get(bin_index);
    }

    public TechnicalBin getBinByindex(int bin_index){
        return tbs.get(bin_index);
    }//根据bin在IBF内的索引获取这个bin对象

    public LongBitSet transposeTorow(){//按列存储转换为按行存储
        int numCols=TB_num;//bin数量
        int numRows=tbs.get(0).getSize();//bin大小
        long size = (long) numCols * numRows;
        ibfASrow=new LongBitSet(size);
        for(int i=0;i<numRows;i++){
            for(int j=0;j<numCols;j++){
                boolean bit=tbs.get(j).getbitarray().get(i);
                if (bit){
                    ibfASrow.set((long) i *numCols+j);
                }
            }
        }
        return ibfASrow;
    }

    public List<Integer> queryASrow(String kmer){//按行查询
        List<Integer> bin_indexs=new ArrayList<>();//记录当前ibf中包含查询元素的bin在ibf中的索引
        int k=tbs.get(0).getK();
        int size=tbs.get(0).getSize();
        List<Long> hashValues = Utils.myHash(kmer, k, size);
        BitSet result=new BitSet(TB_num);//记录按位与结果
        result.set(0,TB_num);//都设置为1
        BitSet tempBitSet=new BitSet(TB_num);
        for(long hash_value:hashValues){
            long start_index= hash_value*TB_num;
            long end_index=start_index+TB_num-1;
            //清空临时bitset
            tempBitSet.clear();
            // 复制指定范围内的位到新的 BitSet 中
//            BitSet newBitSet=new BitSet(TB_num);
            for (long i = start_index; i <= end_index; i++) {
//                boolean bitValue = ibfASrow.get(i); // 获取原始 BitSet 中的位值
//                newBitSet.set((int) (i - start_index), bitValue); // 将位值设置到新的 BitSet 中
                tempBitSet.set((int) (i - start_index),ibfASrow.get(i));
            }
//            result.and(newBitSet);
            result.and(tempBitSet);
        }
        for(int i=0;i<TB_num;i++){
            if (result.get(i)){
                bin_indexs.add(i);
            }
        }
        return bin_indexs;
    }

    public List<Integer> queryAScloum(String kmer){//一个个bin的查，即一个个布隆过滤器查
        List<Integer> bin_indexs=new ArrayList<>();//记录当前ibf中包含查询元素的bin在ibf中的索引
        for(int binIndex=0;binIndex<TB_num;binIndex++){
            TechnicalBin query_bin=tbs.get(binIndex);
            if(query_bin.test(kmer)){//当前bin包含查询元素
                bin_indexs.add(binIndex);
            }
        }
        return bin_indexs;//返回包含查询元素的bin在IBF中的索引 即如果一个IBF的bin列表为[a,b,c,d,e]，查询到a，e包含，则返回[0,4]
    }

    public void printInfo() {
        System.out.println("IBF Index: " + ibf_index);
        System.out.println("Parent IBF: " + parent_ibf);
        System.out.println("当前Technical Bins数量: " + TB_num);
        System.out.println("当前ibf存储的Datasets: ");
        System.out.println(datasets);
        System.out.println("每个Technical Bin的情况: ");
        for (TechnicalBin bin : tbs) {
            bin.printInfo();
        }

    }

}
