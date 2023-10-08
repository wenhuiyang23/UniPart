import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import static java.lang.Math.*;

public class WStreamPro {

    private int wsize;

    public int k;
    private int p;

    // global partition information
    public ArrayList<Integer> partitionSize;
    public HashMap<Integer,Integer> partitionRes = new HashMap<>();

    private HashMap<Integer,ArrayList> n_inPartition = new HashMap<Integer,ArrayList>();

    private Triple[] window ;

    private int candidateEdge;

    public int edgeCut;
    private HashMap<Integer,ArrayList> n_inWindow = new HashMap<Integer,ArrayList>();

    public WStreamPro(int windowSize, int pValue, Triple[] win, int kValue ){
        this.wsize = windowSize;
        this.p = pValue;
        this.window = win;
        this.k = kValue;
        this.partitionSize = new ArrayList<>(Collections.nCopies(kValue, 0));
        this.candidateEdge = 0;
    }

    public void initInfoFromWindow(){
        for(int i = 0; i < wsize; i++){
            Triple edge = window[i];
            int v1 = edge.triples[0];
            int v2 = edge.triples[2];

            if(!n_inWindow.containsKey(v1)){
                ArrayList<Integer> list1 = new ArrayList<>();
               list1.add(v2);
               n_inWindow.put(v1,list1);
            }else{n_inWindow.get(v1).add(v2);}

            if(!n_inWindow.containsKey(v2)){
                ArrayList<Integer> list2 = new ArrayList<>();
                list2.add(v1);
                n_inWindow.put(v2,list2);
            }else{n_inWindow.get(v2).add(v1);}

            if(!n_inPartition.containsKey(v1)){
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                n_inPartition.put(v1,value);
            }
            if(!n_inPartition.containsKey(v2)){
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                n_inPartition.put(v2,value);
            }
        }

    }


    private int greedyStrategy(int vertexId, int hLoad) throws Exception {
        int ind;
        int maxN = -1;
        int maxN_ind = -1;
        double maxD = -1;
        int maxD_ind = -1;
        double decision = -2;
        double sum = partitionSize.stream().mapToInt(Integer::intValue).sum();
        ArrayList<Integer> nlist;
        Random random = new Random();
        ArrayList<Integer> n_N_inPartition = n_inPartition.get(vertexId);
        if(n_N_inPartition != null){
            ArrayList<Integer> indices1 = new ArrayList<>();
            ArrayList<Integer> indices2 = new ArrayList<>();
            for(int i = 0; i < k; i++){
            if(i == hLoad) continue;
            int m = n_N_inPartition.get(i);
            if(sum != 0){
                decision= m*(1 - partitionSize.get(i)/sum);
               // decision = m*(2/(1+exp(partitionSize.get(i)/(double)100000)));
            }
            if(m>maxN){
                maxN = m;
                indices1.clear();
                indices1.add(i);
            } else if (m == maxN) {
                indices1.add(i);
            }
                if (decision>maxD){
                maxD = decision;
                indices2.clear();
                indices2.add(i);
                } else if (decision == maxD) {
                    indices2.add(i);
                }
            }
            if(indices1.size()>1){
                int minSize = Integer.MAX_VALUE;
                ArrayList<Integer> smallestIndices = new ArrayList<>();
                for(int i : indices1) {
                    if(partitionSize.get(i) < minSize) {
                        minSize = partitionSize.get(i);
                        smallestIndices.clear();
                        smallestIndices.add(i);
                    } else if(partitionSize.get(i) == minSize) {
                        smallestIndices.add(i);
                    }
                }
                maxN_ind = smallestIndices.get(random.nextInt(smallestIndices.size()));

            }else if(indices1.size() == 1) {
                maxN_ind = indices1.get(0);
            }
            if(indices2.size()>1){
                int minSize = Integer.MAX_VALUE;
                ArrayList<Integer> smallestIndices = new ArrayList<>();
                for(int i:indices2) {
                    if(partitionSize.get(i) < minSize) {
                        minSize = partitionSize.get(i);
                        smallestIndices.clear();
                        smallestIndices.add(i);
                    } else if(partitionSize.get(i) == minSize) {
                        smallestIndices.add(i);
                    }
                }
                maxD_ind = smallestIndices.get(random.nextInt(smallestIndices.size()));

            }else if(indices2.size() == 1) {
                maxD_ind = indices2.get(0);
            }
        }else {
            maxN = 0;
            maxN_ind = random.nextInt(k);
            maxD_ind = partitionSize.indexOf(Collections.min(partitionSize));
        }

        nlist = n_inWindow.get(vertexId);
        double nsize;
        double nsizeProp;
        double maxNpar;
        if(nlist == null) nsizeProp = 0;
        else nsizeProp = nlist.size()/(double)wsize;

        if(sum/(double)k > wsize) {
            nsize = nsizeProp;
            maxNpar = maxN/(double)partitionSize.get(maxN_ind);
        }else {
            nsize = nlist.size();
            maxNpar = maxN;
        }

        if(nsize <= maxNpar) {
            ind = maxN_ind;
        }
        else {
            if(Collections.max(partitionSize)==0) {
                ind = random.nextInt(k);
            }
            else {
                ind = maxD_ind;
            }

        }
        // update partition result, partitionSize
        // update n_inPartition/n_inWindow which means inform all its neighbours
        // delete its information in n_inPartition and in n_inWindow
        partitionRes.put(vertexId,ind);
        partitionSize.set(ind, partitionSize.get(ind) + 1);

        if(nlist != null) {
            for (int i : nlist) {
                ArrayList<Integer> list = n_inPartition.get(i);
                int value = list.get(ind);
                list.set(ind, value + 1);
                n_inPartition.put(i, list);
                n_inWindow.get(i).remove(Integer.valueOf(vertexId));
            }
        }
        n_inPartition.remove(vertexId);
        n_inWindow.remove(vertexId);
        return ind;

    }

    public void partition() throws Exception {
        int v1 = window[candidateEdge].triples[0];
        int v2 = window[candidateEdge].triples[2];
        boolean winHasV1 = n_inWindow.containsKey(v1);
        boolean winHasV2 = n_inWindow.containsKey(v2);
        int ind_v1 = -1;
        int ind_v2 = -1;
        double deviation = Collections.max(partitionSize)-Collections.min(partitionSize);
        //System.out.println(deviation);
        int hLoad = -1;

        if (v1 <= 0){
                ind_v1 = -v1;
        }else if(v1 > 0 && winHasV1){
            if (deviation >= p){ hLoad = partitionSize.indexOf(Collections.max(partitionSize));}
            ind_v1 = greedyStrategy(v1, hLoad);
        }else if(v1 > 0 && !winHasV1){
            ind_v1 = partitionRes.get(v1);
        }

        if (v2 <= 0){
            ind_v2 = -v2;
        }else if(v2 > 0 && winHasV2){
            if (deviation >= p){ hLoad = partitionSize.indexOf(Collections.max(partitionSize));}
            ind_v2 = greedyStrategy(v2, hLoad);
        }else if(v2 > 0 && !winHasV2){
            ind_v2 = partitionRes.get(v2);
        }

        if(ind_v1 != ind_v2){
            edgeCut += 1 ;
        }

    }
    public void update(Triple e){
        int v1 = e.triples[0];
        int v2 = e.triples[2];
        boolean parHasV1 = partitionRes.containsKey(v1);
        boolean parHasV2 = partitionRes.containsKey(v2);
        boolean n_winHasV1 = n_inWindow.containsKey(v1);
        boolean n_winHasV2 = n_inWindow.containsKey(v2);
        boolean n_parHasV1 = n_inPartition.containsKey(v1);
        boolean n_parHasV2 = n_inPartition.containsKey(v2);

        if(parHasV1 && !parHasV2){
            int ind_v1 = partitionRes.get(v1);
            e.triples[0] = -ind_v1;
            if(!n_winHasV2){
                n_inWindow.put(v2,new ArrayList<>());
            }
            if(n_parHasV2){
                ArrayList<Integer> list = n_inPartition.get(v2);
                int value = list.get(ind_v1);
                list.set(ind_v1, value + 1 );
                n_inPartition.put(v2, list);
            }else{
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                value.set(ind_v1,1);
                n_inPartition.put(v2,value);
            }
        }
        if(!parHasV1 && parHasV2){
            int ind_v2 = partitionRes.get(e.triples[2]);
            e.triples[2] = -ind_v2;
            if(!n_winHasV1){
                n_inWindow.put(v1,new ArrayList<>());
            }
            if(n_parHasV1){
                ArrayList<Integer> list = n_inPartition.get(v1);
                int value = list.get(ind_v2);
                list.set(ind_v2, value + 1 );
                n_inPartition.put(v1, list);
            }else{
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                value.set(ind_v2,1);
                n_inPartition.put(v1,value);
            }
        }
        if(parHasV1 && parHasV2){
            int ind_v1 = partitionRes.get(v1);
            int ind_v2 = partitionRes.get(v2);
            e.triples[0] = -ind_v1;
            e.triples[2] = -ind_v2;
        }
        if(!parHasV1 && !parHasV2){
            if(!n_winHasV1){
                ArrayList<Integer> value1 = new ArrayList<>();
                value1.add(v2);
                n_inWindow.put(v1,value1);
            }else{n_inWindow.get(v1).add(v2);}

            if(!n_winHasV2){
                ArrayList<Integer> value2 = new ArrayList<>();
                value2.add(v1);
                n_inWindow.put(v2,value2);
            }else{n_inWindow.get(v2).add(v1);}

            if(!n_parHasV1){
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                n_inPartition.put(v1,value);
            }
            if(!n_parHasV2){
                ArrayList<Integer> value = new ArrayList<>(Collections.nCopies(k, 0));
                n_inPartition.put(v2,value);
            }
        }
        window[candidateEdge] = e;
        candidateEdge = (candidateEdge+1) % wsize;

    }


}