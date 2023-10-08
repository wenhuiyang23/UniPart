
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;


public class Test {

    private static final int BASE_PREDICATE_CODE = 1000000000;
    private HashMap<Integer, Integer> PredicatesAsSubject;
    final static boolean quad = false;
    Dictionary reverseDictionary;
    ArrayList<String> header;

    private Dictionary dictionary;
    private int skipToLine = 0;
    HashMap<String, String> prefix = new HashMap<String, String>();
    private int errQuadProcess = 0;
    private static ArrayList<Triple> flow = new ArrayList<>();

    public void process(ArrayList<String> filePathList, boolean quad) {
        header = new ArrayList();
        PredicatesAsSubject = new HashMap();
        int[] code = new int[3];
        int nextCode = 1;
        int nextPredicateCode = BASE_PREDICATE_CODE;
        Integer errCount = 0;
        Integer errSolved = 0;
        int duplicateCount = 0;
        int count = 0;
        for(int k =0 ; k < filePathList.size() ; k++) {
            File file = new File(filePathList.get(k));
            LineIterator it = null;
            try {
                it = FileUtils.lineIterator(file, "US-ASCII");
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            try {
                while (it.hasNext() && count < 40000000) {
                    if (count % 100000 == 0 || count == 2082) {
                        File stopFile = new File("stop");
                        if(stopFile.exists()){
                            System.out.println("stop file detected, stopping ..");
                            finish();
                            System.exit(0);
                        }
                        if (count == 0)
                            printBuffer(0, k+" processing line " + count / 1000 + " k");
                        else
                            printBuffer(1, k+" processing line " + count / 1000 + " k");
                        checkMemory(false);

                    }
                    count++;
                    String line = it.nextLine();
                    if(count < skipToLine)
                        continue;
                    if (line.startsWith("@")) {
                        if (line.startsWith("@base"))
                            prefix.put("base", line.split(" ")[1]);
                        else
                            prefix.put(line.split(" ")[1].replace(":", ""), line.split(" ")[2]);
                        header.add(line);
                        continue;
                    }
                    //dummy line
                    if(line.contains("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Chair"))
                        line.trim();

                    String[] triple;
                    if (quad) {
                        triple = getTripleFromQuadLine(line , header , prefix);
                        if (triple == null) {
                            errQuadProcess++;
                            continue;
                        }
                    } else {
                        triple = getTripleFromTripleLine(line, errCount, errSolved , header , prefix);
                        if(line.contains("#Person"))
                            line.contains("");

                    }
                    if (triple == null || triple.length < 3) {
                        continue;
                    }
                    if (triple[0].equals(triple[1]))
                        continue;

                    boolean ignoreFlag = false;
                    for (int i = 0; i < 3; i++) {
                        if (dictionary.containsKey(triple[i] , true)) {
                            code[i] = dictionary.get(triple[i],true);
                            if (i != 1 && code[i] >= BASE_PREDICATE_CODE) {
                                //ignoreFlag = true; //just ignore the predicate which came as subj or obj
                                if (!PredicatesAsSubject.containsKey(code[i])) {
                                    PredicatesAsSubject.put(code[i], nextCode);
                                    //v = new ArrayList();
                                    //TODO replace the verticesID list with iteration over reverse dictinary
                                    ///                     vertecesID.add(nextCode);
                                    //addToGraph(nextCode, v);
                                    //graph.put(nextCode, v);
                                    code[i] = nextCode;
                                    dictionary.put(triple[i], code[i]);
                                    nextCode++;
                                }
                            }
                            if (i == 1 && code[i] < BASE_PREDICATE_CODE) {
                                //wrong value was added fix it!
                                //dictionary.remove(triple[i]);
                                //reverseDictionary.remove(code[i]);
                                // vertecesID.remove(code[i]);
                                // graph.remove(code[i]);

                                //    code[i] = nextPredicateCode;
                                //   nextPredicateCode++;
                                //  dictionary.put(triple[i], code[i]);
                                // reverseDictionary.put(code[i], triple[i]);
                            }

                            // v = graph.get(code[i]);
                        } else {
                            if (i != 1) {
                                code[i] = nextCode;
                                nextCode++;

                            } else {
                                code[i] = nextPredicateCode;
                                nextPredicateCode++;
                            }
                            if(count>=skipToLine ) {
                                dictionary.put(triple[i], code[i]);
                            }
                        }
                    }
                    if (ignoreFlag)
                        continue;

                    if(count < skipToLine )
                        continue;
                    if (count % 10000000 == 0) {
                        System.out.println("writing to diskDb");

                    }
                    //build the tripe graph
                    Triple tripleObj;

                    tripleObj = new Triple(code[0], code[1], code[2]);

                    flow.add(tripleObj);

                }

            } finally {
                LineIterator.closeQuietly(it);
            }

        }

    }

    public Test(){
        dictionary = new Dictionary("dictionary_int");
        reverseDictionary = dictionary;

    }

    public static void main(String[] args) throws Exception {
        int windowSize = 1000;
        int p = 600;
        int k = 4;

        Test t = new Test();
        try {
            ArrayList<String> filePaths = new ArrayList<String>();
            filePaths.add("/home/wenhui/lubm/merged10.nq");
            //filePaths.add("/home/wenhui/yagoInfoboxAttributes_en.ttl");

            try {

                t.process(filePaths, true);

        //FileWriter fw = new FileWriter("output.txt");
        //BufferedWriter bufferWriter = new BufferedWriter(fw);


                double edgeCutRatio = 0.0;

                ArrayList<Triple> flowUniq = t.removeDuplicates(flow);

            Collections.shuffle(flowUniq);
            List<Triple> sublist = flowUniq.subList(0, Math.min(flowUniq.size(), windowSize));
            Triple[] window = sublist.toArray(new Triple[windowSize]);
                Runtime r = Runtime.getRuntime();
                r.gc();
                Thread.sleep(100);
                MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
                MemoryUsage beforeMemoryUsage = memoryMXBean.getHeapMemoryUsage();

                WStreamPro demo = new WStreamPro(windowSize, p, window, k);
                demo.initInfoFromWindow();
                long startTime = System.currentTimeMillis();
                for (int i = windowSize; i < flowUniq.size(); i++) {
                    demo.partition();

                    demo.update(flowUniq.get(i));

                }
                for (int i = 0; i < windowSize; i++) {
                    demo.partition();

                    demo.update(new Triple(-1, -1, -1));
                }
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                r.gc();
                Thread.sleep(100);
                MemoryUsage afterMemoryUsage = memoryMXBean.getHeapMemoryUsage();
                long memoryUsed = afterMemoryUsage.getUsed() - beforeMemoryUsage.getUsed();
                System.out.println("running：" + duration + " ms");
                System.out.println("max memory used：" + memoryUsed/ (1024.0 * 1024.0) + " mb");



               /* JFreeChart chart = ChartFactory.createLineChart(
                        "Line Chart Example",
                        "windowsize",
                        "time",
                        dataset,
                        PlotOrientation.VERTICAL,
                        false,true,
                        false
                );
                ChartPanel chartPanel = new ChartPanel(chart);
                JFrame frame = new JFrame("Line Chart Example");
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                frame.setSize(500, 400);
                frame.getContentPane().add(chartPanel);
                frame.setVisible(true); */

                // output file
                /*for (Map.Entry<Integer, Integer> entry : demo.partitionRes.entrySet()) {
                    int key = entry.getKey();
                    int value = entry.getValue();
                    bufferWriter.write(key + " " + value);
                    bufferWriter.newLine();
                }
                bufferWriter.close();
                fw.close();*/

                edgeCutRatio = (double) demo.edgeCut / flowUniq.size();
                double sum = demo.partitionSize.stream().mapToInt(Integer::intValue).sum();
                double loadBalance = Collections.max(demo.partitionSize) / (sum / k);

                System.out.println("edgeRatio " + edgeCutRatio);
                System.out.println("loadbalanceRatio " + loadBalance);
                System.out.println(demo.partitionSize);


                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }catch (Exception e){
                    e.printStackTrace();
                    t.finish();
                }
            }

    public ArrayList<Triple> removeDuplicates(ArrayList<Triple> data) {
        ArrayList<Triple> flownew = new ArrayList<>();
        HashMap<String, Boolean> map = new HashMap<>();
        for (Triple triple : data) {
            String key1 = triple.triples[0] + " " + triple.triples[2];
            String key2 = triple.triples[2] + " " + triple.triples[0];
            if (!map.containsKey(key1)) {
                if(!map.containsKey(key2)){
                    flownew.add(triple);
                    map.put(key1, true);
                    map.put(key2, true);
                }else{map.put(key1, true);
                }
            }else{
                if(!map.containsKey(key2)) map.put(key2, true);
            }
        }
        return flownew;
    }

    static ArrayList<String> printBuffer;

    static public void printBuffer(int countToDelete, String s) {
        if (printBuffer == null)
            printBuffer = new ArrayList();
        for (int i = 0; i < countToDelete; i++) {
            if (printBuffer.size() > 0)
                printBuffer.remove(printBuffer.size() - 1);
            else
                break;
        }
        printBuffer.add(s);
        for (int i = 0; i < 50; i++) {
            System.out.println();
        }
       /* try {
            Runtime.getRuntime().exec("clear");
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        for (int i = 0; i < printBuffer.size(); i++) {
            System.out.println(printBuffer.get(i));
        }

    }
    private void finish() {
        if(dictionary != null)
            dictionary.close();
    }

    public static String[] getTripleFromQuadLine(String line , ArrayList<String> header , HashMap<String,String> prefix) {
        if(line.contains("<http://purl.org/rss/1.0/modules/content/encoded>"))
            line.contains("");
        String t = line.replace("qqq","");
        if (!line.startsWith("<") && !line.startsWith("_:")) {
            /*errQuadProcess++;
            startErrQuadProcess++;*/
            return null;
        }
        String[] triple = new String[3];
        String [] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet , header ,prefix);
        triple[1] = stripItem(linet , header , prefix);
        triple[2] = stripItem(linet , header , prefix);
        if(triple[2] != null)
            triple[2] = triple[2]+'.';
        if(triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        //startErrQuadProcess++;
        return null;
    }
    public static String[] getTripleFromTripleLine(String line, Integer errCount, Integer errSolved , ArrayList<String > header , HashMap<String,String> prefix) {
        String[] triple = new String[3];
        String [] linet = new String[1];
        linet[0] = line;
        triple[0] = stripItem(linet , header ,prefix);
        triple[1] = stripItem(linet , header , prefix);
        triple[2] = stripItem(linet , header , prefix);
        if(triple[2] != null)
            triple[2] = triple[2];
        if(triple[0] != null && triple[1] != null && triple[2] != null)
            return triple;
        errCount++;
        return null;
    }

    public static String stripItem(String []linet , ArrayList<String > header , HashMap<String,String> prefix){
        String line = linet[0].trim();
        String item = null;
        char startChar;
        if (line.length() > 0) {
            startChar = line.charAt(0);

        } else {

            return null;
        }
        switch (startChar){
            case '\"':
                line = line.substring(1);
                for (int i = 1; i < line.length(); i++){
                    char c = line.charAt(i);
                    if(c == '\"' && line.charAt(i-1) != '\\' ) {
                        item = line.substring(0, i + 1);
                        line = line.substring(i+1);
                        linet[0] = line;
                        return "\""+item;
                    }
                }
                if(line.startsWith("\""))
                    return "\""+"\"";
                return null;

            case '<':
                item = line.substring(0,line.indexOf('>')+1);
                line = line.substring(line.indexOf('>')+1);
                linet[0] = line;
                return item;
            case '_':
                item = line.substring(0,line.indexOf(' ')+1);
                line = line.substring(line.indexOf(' ')+1);
                linet[0] = line;
                return item;
        }
        String []arr = line.split(":");
        if(arr.length > 0){
            for(int i = 0 ; i < header.size() ; i++){
                if(prefix.containsKey(arr[0])){
                    char endChar;
                    if(line.contains(" "))
                        endChar = ' ';
                    else if(line.contains("}"))
                        endChar = '}';
                    else if(line.contains("."))
                        endChar = '.';
                    else
                        return null;
                    item = line.substring(0,line.indexOf(endChar)+1);
                    line = line.substring(line.indexOf(endChar)+1);
                    linet[0] = line;
                    return item.trim();

                }
                return line;
            }
        }
        return null;
    }
    public static long checkMemory(boolean log){
        long rem =  Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long max = Runtime.getRuntime().maxMemory();
        if(log) {
            System.out.println(" app used memory: " + rem / 1000000000 + " GB");
            System.out.println(" app used memory: " + rem / 1000 + " KB");
        }
        if((max-rem)/1000 < 2000000) {
            System.out.println(" Low memory detected... ");
        }
        return rem/1000; //kB
    }
}
