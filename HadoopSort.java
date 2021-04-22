import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HadoopSort {

  private static final Logger logger = Logger.getLogger(HadoopSort.class);
  //Total number of ascii characters in generated gensort
  private static final int tchar = 128;
  public static class keyValueMapper
       extends Mapper<Object, Text, Text, Text>{

    public void mapping(Object key, Text value, Context context) throws IOException, InterruptedException {

      String line  = value.toString();
      String keypair = line.substring(0,10);
      String valuepair = line.substring(11,98);
        valuepair += "\r";
      context.write(new Text(keypair),new Text(valuepair));
    }
  }

  //Reducer that just writes to files
  public static class FileReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text keyR, Iterable<Text> valueR,Context context
      ) throws IOException, InterruptedException {
      for (Text valR : valueR) {

        context.write(keyR,valR);
      }
    }
  }


  //to get sorted output among the partitions
  //Directs the output to reducers based on first character of the string.
  public static class Sortedoutput extends Partitioner<Text,Text>{
                public int getPartition(Text keyS, Text valueS, int numRTasks){
      int numChar = tchar/numReduceTasks;
      int startChar = (int)keyS.toString().charAt(0);
      int iteration = 0;
      while(iteration < numRTasks){
        int start = iteration * numChar;
        int end = (iteration+1) * numChar;
        if(startChar >= start && startChar < end){
          return iteration;
        }
          iteration++;
      }
      return iteration-1;
    }
  }


  public static void main(String[] args) throws Exception {
    logger.info("Starting timer");
    long begin = System.currentTimeMillis();
    Configuration config = new Configuration();
    Job job = Job.getInstance(config, "Hadoop Sort");
    job.setJarByClass(HadoopSort.class);
    job.setMapperClass(keyValueMapper.class);
    job.setCombinerClass(FileReducer.class);
    job.setPartitionerClass(Sortedoutput.class);
    //Setting number of reducers
    job.setReducerClass(FileReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    if(job.waitForCompletion(true) == true){
      long end = System.currentTimeMillis();
            long timeE = end - begin;
            logger.info("Time elapsed : \n");
            logger.info(timeE);
            logger.warn("===============================================================");
        System.exit(0);
    }
    else{
        System.exit(1);
    }
  }
}