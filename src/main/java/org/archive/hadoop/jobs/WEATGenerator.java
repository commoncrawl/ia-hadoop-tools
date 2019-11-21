package org.archive.hadoop.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.archive.extract.ExtractingResourceFactoryMapper;
import org.archive.extract.ExtractingResourceProducer;
import org.archive.extract.ExtractorOutput;
import org.archive.extract.ProducerUtils;
import org.archive.extract.ResourceFactoryMapper;
import org.archive.extract.WATExtractorOutput;
import org.archive.extract.WETExtractorOutput;
import org.archive.hadoop.util.FilenameInputFormat;
import org.archive.resource.Resource;
import org.archive.resource.ResourceProducer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WEATGenerator - Generate WAT and WET files from (W)ARC files stored in HDFS
 */
public class WEATGenerator extends Configured implements Tool {

  public final static String TOOL_NAME = "WEATGenerator";
  public final static String TOOL_DESCRIPTION = "Generate WAT and WET files from (W)ARC files stored in HDFS";

  public static final Log LOG = LogFactory.getLog(WEATGenerator.class);

  public static class WEATGeneratorMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
    private JobConf jobConf;

    /**
     * <p>Configures the job.</p>
     * * @param job The job configuration.
     */
    public void configure( JobConf job ) {
      this.jobConf = job;
    }

    /**
     * Generate WAT file for the (w)arc file named in the
     * <code>key</code>
     */
    public void map( Text key, Text value, OutputCollector output, Reporter reporter )throws IOException {
      String path = key.toString();
      LOG.info( "Start: "  + path );

      try {
        Logger.getLogger("org.archive.format.gzip.GZIPMemberSeries").setLevel(Level.WARNING);
        Logger.getLogger("org.archive.extract.ExtractingResourceProducer").setLevel(Level.WARNING);

        Path inputPath = new Path(path);
        Path basePath = inputPath.getParent().getParent();

        String inputBasename = inputPath.getName();
        String watOutputBasename = "";
        String wetOutputBasename = "";

        if(path.endsWith(".gz")) {
          watOutputBasename = inputBasename.substring(0,inputBasename.length()-3) + ".wat.gz";
          wetOutputBasename = inputBasename.substring(0,inputBasename.length()-3) + ".wet.gz";
        } else {
          watOutputBasename = inputBasename + ".wat.gz";
          wetOutputBasename = inputBasename + ".wet.gz";
        }

        String watOutputFileString = basePath.toString() + "/wat/" + watOutputBasename;
        String wetOutputFileString = basePath.toString() + "/wet/" + wetOutputBasename;

        LOG.info("About to write out to " + watOutputFileString + " and " + wetOutputFileString);   
        if (this.jobConf.getBoolean("skipExisting", false)) {
        	FileSystem fs = FileSystem.get(new java.net.URI(watOutputFileString), this.jobConf);
        	if (fs.exists(new Path(watOutputFileString)) && fs.exists(new Path(wetOutputFileString))) {
        		LOG.info("Skipping " + inputBasename + " wet & wat already exist and skipExisting=true");
        		return;
        	}
        }

        FSDataOutputStream watfsdOut = FileSystem.get(new java.net.URI(watOutputFileString), this.jobConf).create(new Path(watOutputFileString), false);
        ExtractorOutput watOut = new WATExtractorOutput(watfsdOut);
        ResourceProducer producer = ProducerUtils.getProducer(path.toString());
        ResourceFactoryMapper mapper = new ExtractingResourceFactoryMapper();
        ExtractingResourceProducer exProducer = new ExtractingResourceProducer(producer, mapper);

        FSDataOutputStream wetfsdOut = FileSystem.get(new java.net.URI(wetOutputFileString), this.jobConf).create(new Path(wetOutputFileString), false);
        ExtractorOutput wetOut = new WETExtractorOutput(wetfsdOut, wetOutputBasename);

        int count = 0;
        while(count < Integer.MAX_VALUE) {
          Resource r = exProducer.getNext();
          if(r == null) {
            break;
          }
          count++;
          reporter.incrCounter("exporter", "processed", 1);
          if (count % 1000 == 0) {
        	  LOG.info("Outputting new record " + count);
          }
          wetOut.output(r);
          watOut.output(r);
        }
        watfsdOut.close();
        wetfsdOut.close();
      } catch ( Exception e ) {
        LOG.error( "Error processing file: " + path, e );
        reporter.incrCounter("exporter", "errors", 1);
        if ( this.jobConf.getBoolean( "strictMode", true ) ) {
          throw new IOException( e );
        }
      } finally {
        LOG.info( "Finish: "  + path );
      }

    }

  }

  /**
   * Run the job.
   */
  public int run( String[] args ) throws Exception {
    if ( args.length < 2 ) {
      usage();
      return 1;
    }

    // Create a job configuration
    JobConf job = new JobConf( getConf( ) );

    // The inputs are a list of filenames, use the
    // FilenameInputFormat to pass them to the mappers.
    job.setInputFormat( FilenameInputFormat.class );

    // This is a map-only job, no reducers.
    job.setNumReduceTasks(0);

    // set timeout to a high value - 20 hours
    job.setInt("mapred.task.timeout",72000000);

    // keep job running despite some failures in generating WATs
    job.setBoolean("strictMode",false);
    job.setBoolean("skipExisting", false);

    job.setOutputFormat(NullOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(WEATGeneratorMapper.class);
    job.setJarByClass(WEATGenerator.class);

    int arg = 0;
    while (args[arg].startsWith("-")) {
    	if(args[arg].equals("-strictMode")) {
    		job.setBoolean("strictMode",true);
    		arg++;    		
    	} else if(args[arg].equals("-skipExisting")) {
    		job.setBoolean("skipExisting", true);
    		arg++;
    	} else {
    		break;
    	}
    }


    String randomId = args[arg];
    arg++;
    
    // Job name uses output dir to help identify it to the operator.
    job.setJobName( "WEAT Generator " + randomId);

    //FileOutputFormat.setOutputPath(job, new Path(outputDir));

    boolean atLeastOneInput = false;
    for (int i = arg;i < args.length; i++) {
      FileSystem inputfs = FileSystem.get( new java.net.URI( args[i] ), getConf() );
      for ( FileStatus status : inputfs.globStatus( new Path( args[i] ) ) ) {
        Path inputPath  = status.getPath();
        atLeastOneInput = true;
        LOG.info( "Add input path: " + inputPath );
        FileInputFormat.addInputPath(job, inputPath);
      }
    }
    if (! atLeastOneInput) {
      LOG.info( "No input files to WEATGenerator." );
      return 0;
    }

    // Run the job!
    RunningJob rj = JobClient.runJob(job);
    if ( ! rj.isSuccessful( ) ) {
      LOG.error( "FAILED: " + rj.getID() );
      return 2;
    }
    return 0;
  }

  /**
   * Emit usage information for command-line driver.
   */
  public void usage( ) {
    String usage = "Usage: WEATGenerator <batch identifier> <(w)arcfile>...\n" ;
    System.out.println( usage );
  }

  /**
   * Command-line driver.  Runs the WEATGenerator as a Hadoop job.
   */
  public static void main( String args[] ) throws Exception {
    int result = ToolRunner.run(new Configuration(), new WEATGenerator(), args);
    System.exit( result );
  }
}
