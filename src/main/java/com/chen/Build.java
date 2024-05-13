package com.chen;

import com.clearspring.analytics.util.IBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Collections;

public class Build {
    private static int cur_level_index;
    private static int cur_ibf_index;
    private static int h=2;//哈希函数个数
    private static int tmax;
    private static double pfpr=0.05;//假阳性率

    private static class MergeBin{
        int IBFindex;
        int BINindex;//bin在IBF内的索引

        public MergeBin(int ibf_index,int bin_index){
            this.IBFindex=ibf_index;
            this.BINindex=bin_index;
        }
        public int getIBFindex() {
            return IBFindex;
        }
        // 获取 BINindex 的 getter 方法
        public int getBINindex() {
            return BINindex;
        }
    }

    static {
        cur_level_index=0;
        cur_ibf_index=0;
    }

    public static void buildFromSER(){
        String index_serFIle="D:/Code/Idea_Codes/HIBF_FILE/serializeFIle/"+"hibf_index.ser";
        HIBF.deserialize(index_serFIle);
        String metadata_serFIle="D:/Code/Idea_Codes/HIBF_FILE/serializeFIle/"+"meta.ser";
        Metadata.deserialize(metadata_serFIle);
    }

    public static void buildIndex() throws IOException {
        //记录当前构建层的merge_bin
        List<MergeBin> curlevel_mergebins=new ArrayList<>();
        //构建第0层
        curlevel_mergebins= Build_top_level();//第0层的merge bin
        cur_level_index++;
        cur_ibf_index++;
        while(!curlevel_mergebins.isEmpty()){//知道当前层没有mergin bin，HIBF不需要继续向下构建
            List<MergeBin> nextlevel_mergebins=new ArrayList<>();//记录下一层的mergin bin
            for(MergeBin mb:curlevel_mergebins){
                //当前层要处理的merge_bin，处理一个merge_bin产生一个ibf
                int ibfIndex=mb.getIBFindex();//这个bin所属的IBF的全局ibf索引
                int mergebinIndex=mb.getBINindex();//这个bin在所属IBF内的bin索引
                //获得这个merge_bin所属的IBF
                IBF cur_ibf=HIBF.getIbfByIndex(ibfIndex);
                TechnicalBin bin=cur_ibf.getBinByindex(mergebinIndex);//获得这个bin
                //为它创建孩子节点
                bin.setChild(cur_ibf_index);
                IBF ibf=new IBF(cur_level_index,cur_ibf_index,ibfIndex);//为这个merge bin创建的孩子节点，每个孩子节点都是一个IBF

                List<Integer> datasetsIndexs=cur_ibf.getDatasetsByBinindex(mergebinIndex);//这个mergin bin存储的数据集列表
//                System.out.println("当前存储的数据集"+datasetsIndexs);
                List<Integer> cardinalitys=Metadata.getCardinalities(datasetsIndexs);//存储的这些数据集对应的基数，从大到小排序
//                System.out.println("对应基数"+cardinalitys);
                //新构建的这个孩子节点IBF的tmax
                tmax= (int) Math.ceil(Math.sqrt(datasetsIndexs.size()));
                //对应的基础基数
                int basic_cardinality=compulate_basic_cardinality(cardinalitys,tmax);
                int single_bin_size= (int) (-1 * (h * basic_cardinality) / Math.log(1 - Math.pow(pfpr, 1.0 / h)));

                for(int index=0;index<datasetsIndexs.size();){
                    //依次获取mergin bin中存储的数据集及基数，基数从大到小，这些数据集也是孩子节点IBF中要存储的数据集
                    int dataset_index=datasetsIndexs.get(index);
                    int cardinality=cardinalitys.get(index);
                    if(cardinality>basic_cardinality){//需要划分，构建split_bin
                        int bin_num=cardinality/basic_cardinality+1;//需要的bin数量
//                        System.out.println(dataset_index+"在第"+cur_level_index+"层需要"+bin_num+"个bin");
                        List<List<Integer>> datasets= Collections.nCopies(bin_num, Collections.singletonList(dataset_index));//每个bin对应它存储的数据集索引列表，由于这是split，所有连续bin_num存储的都是这个数据集
                        //将元素存入这bin_num个bin中
                        List<TechnicalBin> cur_bins=new ArrayList<>();
                        for(int i=0;i<bin_num;i++){//创建指定数目的bin
                            cur_bins.add(new TechnicalBin(single_bin_size,h,cur_ibf_index,"split", Collections.singletonList(dataset_index)));
                        }
                        //暂存每组的kmer，从而保证一个个布隆过滤器插入，避免插入的时候在多个布隆过滤器间跳转
                        // 创建一个包含 bin_num 个元素的集合
                        List<Set<String>> binSetList = new ArrayList<>();
                        // 初始化每个元素为一个 Set
                        for (int i = 0; i < bin_num; i++) {
                            binSetList.add(new HashSet<>());
                        }
                        //获取文件路径
                        String filepath=Metadata.getDatapathByIdx(dataset_index);//文件地址
                        //读取kmers存储这几个bin中
                        try (BufferedReader br=new BufferedReader(new FileReader(filepath))){
                            String kmer;
                            int setIndex=0;//当前分配的bin索引
                            while ((kmer=br.readLine())!=null){
                                Set<String> selectSet=binSetList.get(setIndex);
                                if (selectSet.size()<basic_cardinality){
                                    binSetList.get(setIndex).add(kmer);
                                    setIndex=(setIndex+1)%bin_num;
                                }

                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        //分别把每个组的kmer存储到对应的bin
                        for(int binIndex=0;binIndex<bin_num;binIndex++){
                            cur_bins.get(binIndex).batchInsertElements(binSetList.get(binIndex));
                        }
                        ibf.addBins(cur_bins,datasets);//添加到ibf中
//                ibf.printInfo();
                        index++;//获取下一个数据集及其基数
                    }else{//看看能不能合并 不是存储在single_bin就是存储在merge_bin中
                        int sum=cardinality;//当前元素，查看能否与其后元素合并
                        List<Integer> datasets=new ArrayList<>();//如果是single_bin，则包含一个数据集，如果是merge_bin，则包含多个数据集
                        datasets.add(dataset_index);
                        for(int j=index+1;j<cardinalitys.size();j++){
                            int next_dataset_index=datasetsIndexs.get(j);//数据集索引
                            int next_cardinality=cardinalitys.get(j);//基数
                            sum+=next_cardinality;
                            if(sum<=basic_cardinality){
                                datasets.add(next_dataset_index);
                                index++;
                            }else{
                                break;
                            }
                        }
                        TechnicalBin cur_bin;
                        //将datasets中的数据集存储在一个bin中
                        if (datasets.size()==1){
                            cur_bin=new TechnicalBin(single_bin_size,h,cur_ibf_index,"single", datasets);
                        }else{//merge_bin
                            cur_bin=new TechnicalBin(single_bin_size,h,cur_ibf_index,"merge", datasets);
                            //当前bin在ibf中的索引
                            int bin_index=ibf.getTB_num();
                            nextlevel_mergebins.add(new MergeBin(cur_ibf_index,bin_index));
                        }
                        Set<String> all_kmers=new HashSet<>();
                        for(int datasetindex:datasets){
                            //获取文件路径
                            String filepath=Metadata.getDatapathByIdx(datasetindex);//文件地址
                            //读取kmers存储这几个bin中
                            try (BufferedReader br=new BufferedReader(new FileReader(filepath))){
                                String kmer;
                                while ((kmer=br.readLine())!=null){
                                    all_kmers.add(kmer);
                                }
                            }
                        }
                        cur_bin.batchInsertElements(all_kmers);
                        ibf.addBins(Collections.singletonList(cur_bin), Collections.singletonList(datasets));//添加到ibf中
//                ibf.printInfo();
                        index++;//获取下一个数据集及其基数

                    }
                }
                curlevel_mergebins=nextlevel_mergebins;//将新的一层的mergin bin赋给curlevel_mergebins
                HIBF.add_level(cur_level_index, cur_ibf_index ,ibf);
                cur_ibf_index++;
            }
            cur_level_index++;
        }
//        HIBF.printInfo();
    }

    public static void serializeAll(){//序列化HIBF索引和元数据
        String index_serFIle="D:/Code/Idea_Codes/HIBF_FILE/serializeFIle/"+"hibf_index.ser";
        HIBF.serialize(index_serFIle);
        String metadata_serFIle="D:/Code/Idea_Codes/HIBF_FILE/serializeFIle/"+"meta.ser";
        Metadata.serialize(metadata_serFIle);
    }

    public static void dataPreProcessing() throws IOException {//数据预处理
        HyperLogLog.HyperLogLog_estimates();//基数估计
        Metadata.sortCardinality();//基数排序
    }

    public static int compulate_basic_cardinality(List<Integer> cardinality_list,int tmax){
        // 使用平均数作为初始single bin的基数
        double average = cardinality_list.stream()
                .mapToDouble(Integer::doubleValue)
                .average()
                .orElse(0.0);
        //如果使用平均数可以将tb数量控制在tmax下，则直接使用
        int basic_cardinality=(int) Math.round(average);
        while(true){
            int need_bin_num=0;
            for (int index=0;index<cardinality_list.size();) {//依次获取数据集以及对应基数
                int cardinality = cardinality_list.get(index);
                if (cardinality > basic_cardinality) {//需要划分，构建split_bin
                    int bin_num = cardinality / basic_cardinality + 1;//需要的bin数量
                    need_bin_num+=bin_num;
                    index++;
                }else {
                    int sum=cardinality;//当前元素，查看能否与其后元素合并
                    for(int j=index+1;j<cardinality_list.size();j++){
                        int next_cardinality=cardinality_list.get(j);
                        sum+=next_cardinality;
                        if(sum<=basic_cardinality){
                            index++;
                        }else{
                            break;
                        }
                    }
                    need_bin_num++;
                    index++;
                }
            }
            if (need_bin_num<=tmax){
                return basic_cardinality;
            }else{//如果需要的bin数量超过tmax，则上调basic_cardinality
//                System.out.println("调整single_bin的大小");
                basic_cardinality= (int) (1.5*basic_cardinality);
            }
        }
    }

    public static List<MergeBin> Build_top_level() throws IOException {//构建第0层
        //构建第0层
        List<MergeBin> curlevel_mergebins=new ArrayList<>();//记录第0层的merge_bin，这些merge_bin还需要继续向下构建
        List<Integer> cardinalitys=Metadata.getAllValues();//所有数据集的基数，从大到小排序
        //HIBF中每个IBF中的最多technical bin数量
        tmax= (int) Math.ceil(Math.sqrt(cardinalitys.size()));
        //第0层的IBF确定的基础single_bin存储的基数
        int basic_cardinality = compulate_basic_cardinality(cardinalitys,tmax);
//        System.out.println("第0层确定的basiv_cardinality:"+basic_cardinality);
        //基础single_bin对应的布隆过滤器大小，即第0层IBF中每个bin的大小
        int single_bin_size= (int) (-1 * (h * basic_cardinality) / Math.log(1 - Math.pow(pfpr, 1.0 / h)));

        //创建第0层的ibf  第0层只有一个IBF，也是整个HIBF的根节点
        IBF ibf=new IBF(cur_level_index,cur_ibf_index,-1);

        for (int index=0;index<Metadata.getSortedList().size();) {//依次获取数据集以及对应基数
            Map.Entry<Integer,Integer> entry=Metadata.getSortedList().get(index);
            int dataset_index=entry.getKey();//数据集索引
            int cardinality=entry.getValue();//基数
            if(cardinality>basic_cardinality){//需要划分，构建split_bin
                int bin_num=cardinality/basic_cardinality+1;//需要的bin数量
//                System.out.println(dataset_index+"需要"+bin_num+"个bin");
                List<List<Integer>> datasets= Collections.nCopies(bin_num, Collections.singletonList(dataset_index));//每个bin对应它存储的数据集索引列表，由于这是split，所有连续bin_num存储的都是这个数据集
                //如果数据集0需要三个bin，则数据集列表为[[0],[0]，[0]]代表每个bin存储的数据集都是0
                //创建bin_num个bin
                List<TechnicalBin> cur_bins=new ArrayList<>();
                for(int i=0;i<bin_num;i++){//创建指定数目的bin
                    cur_bins.add(new TechnicalBin(single_bin_size,h,cur_ibf_index,"split", Collections.singletonList(dataset_index)));
                }
                // 暂存每组的kmer，从而保证一个个布隆过滤器插入，避免插入的时候在多个布隆过滤器间跳转
                // 创建一个包含 bin_num 个元素的集合列表，每个集合对应一个bin
                List<Set<String>> binSetList = new ArrayList<>();
                // 初始化每个元素为一个 Set
                for (int i = 0; i < bin_num; i++) {
                    binSetList.add(new HashSet<>());
                }
                //获取文件路径
                String filepath=Metadata.getDatapathByIdx(dataset_index);//文件地址
                //读取kmers均匀存储这几个bin中
                try (BufferedReader br=new BufferedReader(new FileReader(filepath))){
                    String kmer;
                    int setIndex=0;//当前分配的bin索引
                    while ((kmer=br.readLine())!=null){
                        Set<String> selectSet=binSetList.get(setIndex);
                        if (selectSet.size()<basic_cardinality){
                            binSetList.get(setIndex).add(kmer);
                            setIndex=(setIndex+1)%bin_num;
                        }

                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                //分别把每个组的kmer存储到对应的bin
                for(int binIndex=0;binIndex<bin_num;binIndex++){
                    cur_bins.get(binIndex).batchInsertElements(binSetList.get(binIndex));
                }
                ibf.addBins(cur_bins,datasets);//将这bin_num个bin添加到ibf中
//                ibf.printInfo();
                index++;//获取下一个数据集及其基数
            }else{//看看能不能合并 不是存储在single_bin就是存储在merge_bin中
                int sum=cardinality;//当前元素，查看能否与其后元素合并
                List<Integer> datasets=new ArrayList<>();//如果是single_bin，则包含一个数据集，如果是merge_bin，则包含多个数据集
                datasets.add(dataset_index);//添加当前数据集，即第index大基数对应的数据集索引
                for(int j=index+1;j<Metadata.getSortedList().size();j++){//从第index+1大基数开始尝试，是否能和第index大基数合并到merge bin
                    Map.Entry<Integer,Integer> next_entry=Metadata.getSortedList().get(j);
                    int next_dataset_index=next_entry.getKey();//数据集索引
                    int next_cardinality=next_entry.getValue();//基数
                    sum+=next_cardinality;
                    if(sum<=basic_cardinality){//可以合并
                        datasets.add(next_dataset_index);
                        index++;
                    }else{
                        break;//不可以继续合并了
                    }
                }
                TechnicalBin cur_bin;//无论是single还是merge，只需要一个bin
                //将datasets中的数据集存储在一个bin中
                if (datasets.size()==1){
                    cur_bin=new TechnicalBin(single_bin_size,h,cur_ibf_index,"single", datasets);
                }else{//merge_bin
                    cur_bin=new TechnicalBin(single_bin_size,h,cur_ibf_index,"merge", datasets);
                    //当前bin在ibf中的索引
                    int bin_index=ibf.getTB_num();//记录这个merge bin在所属IBF中的索引
                    curlevel_mergebins.add(new MergeBin(cur_ibf_index,bin_index));//添加到本层的merge bin信息中
                }
                Set<String> all_kmers=new HashSet<>();//无论是single还是merge，都是存储在一个bin中，这里将这个bin要存储的数据集的kmer合并
                for(int datasetindex:datasets){
                    //依次获取文件路径
                    String filepath=Metadata.getDatapathByIdx(datasetindex);//文件地址
                    try (BufferedReader br=new BufferedReader(new FileReader(filepath))){
                        String kmer;
                        while ((kmer=br.readLine())!=null){
                            all_kmers.add(kmer);
                        }
                    }
                }
                cur_bin.batchInsertElements(all_kmers);//将kmer添加到bin中
                ibf.addBins(Collections.singletonList(cur_bin), Collections.singletonList(datasets));//添加到ibf中
//                ibf.printInfo();
                index++;//获取下一个数据集及其基数
            }
        }

        HIBF.add_level(cur_level_index, cur_ibf_index ,ibf);//将构建好的IBF加入HIBF，每个ibf有一个全局ibf索引，即cur_ibf_index
//        HIBF.printInfo();
        return curlevel_mergebins;
    }
}
