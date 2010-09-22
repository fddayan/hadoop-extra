package org.hadoop.extra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.hadoop.extra.ConfigurationUtils;


public class MultiTableInputFormat extends InputFormat<ImmutableBytesWritable, Result> implements Configurable {
    
    private static final String TABLE_NAMES_DELIMITER = ",";

    public static final String MULTITABLE_CONFIGURATION = "multitable.configuration";

    public static final String MULTITABLE_TABLES = "multitable.tables";

    
    private List<SingleTableFormat> tables; 

    
    public MultiTableInputFormat() {
	tables = new ArrayList<SingleTableFormat>();
	
	
    }
    
    @Override
    public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	return inputFormatForSplit(split).createRecordReader(split, context);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
	List<InputSplit> ret = new ArrayList<InputSplit>();
	
	for (SingleTableFormat singleTableFormat: tables) {
	    ret.addAll(singleTableFormat.getSplits(context));
	}
	
	return ret;
    }
    
    private TableInputFormat inputFormatForSplit(InputSplit split) {
	TableSplit tableSplit = (TableSplit) split;
	
	for (SingleTableFormat singleTableFormat: tables) {
	    if (singleTableFormat.isTable(tableSplit.getTableName())) {
		return singleTableFormat;
	    }
	}
	
	throw new IllegalArgumentException("Cannot find TableInputFormat for table " + Bytes.toString(tableSplit.getTableName()));
    }

    @Override
    public Configuration getConf() {
	return null;
    }

    
    @Override
    public void setConf(Configuration conf) {
	
	
//	tables.add(singleTable);
//	tables.add(singleTable);
	
	String tablesConf = conf.get(MULTITABLE_TABLES);
	
	Validate.notEmpty(tablesConf,"No Tables provided");
	
	String[] tableNames = tablesConf.split(TABLE_NAMES_DELIMITER); 
	
	for (int i=0;i<tableNames.length;i++) {
	    String configurationXml = conf.get("multitable.configuration" + (i+1));
	    Configuration tableConf = ConfigurationUtils.fromXml(configurationXml);
	    SingleTableFormat singleTable = new SingleTableFormat();
	    
	    singleTable.setConf(tableConf);
	    tables.add(singleTable);
	}
    }
    
    public static void addTable(String tableName,Configuration conf,Job job) {
	String tables = job.getConfiguration().get(MULTITABLE_TABLES);
	
	if (StringUtils.isBlank(tables)) {
	    job.getConfiguration().set(MULTITABLE_TABLES,tableName);
	} else {
	    job.getConfiguration().set(MULTITABLE_TABLES,tables + TABLE_NAMES_DELIMITER + tableName);
	}
	
	int tablesSize = job.getConfiguration().get(MULTITABLE_TABLES).split(TABLE_NAMES_DELIMITER).length;
	
	job.getConfiguration().set(MULTITABLE_CONFIGURATION + tablesSize, ConfigurationUtils.toXml(conf));
    }
}
