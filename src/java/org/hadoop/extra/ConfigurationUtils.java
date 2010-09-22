package org.hadoop.extra	;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class ConfigurationUtils {

    public static List<String> getListValues(Configuration conf, String propertyName, String delimiter) {
	List<String> valuesList = new ArrayList<String>();
	
	if (conf == null) return valuesList;
	
	String values = conf.get(propertyName);

	if (StringUtils.isBlank(values)) {
	    return new ArrayList<String>();
	} else {
	    valuesList.addAll(Arrays.asList(values.split(delimiter)));
	    
	    return valuesList;
	}
    }
    
    public static void addFileName(Configuration conf, String fileName) {
	if (StringUtils.isBlank(fileName)) return; 
	addFile(conf,new File(fileName));
    }
    
    public static void addFile(Configuration conf, File file) {
	if (file.exists()) {
	    System.out.println("Adding resource to configuration: " + file.getAbsolutePath());
	    
	    try {
		conf.addResource(file.toURI().toURL());
	    } catch (MalformedURLException e) {
		throw new IllegalArgumentException("File " + file.getAbsolutePath() + " does not exist");
	    }
	} else {
	    throw new IllegalArgumentException("File " + file.getAbsolutePath() + " does not exist");
	}
    }
    
    public static String toXml(Configuration conf) {
	try {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();

	    conf.writeXml(out);

	    String xml = Bytes.toString(out.toByteArray());

	    return xml;
	} catch (IOException e) {
	    e.printStackTrace();
	    
	    throw new RuntimeException(e.getMessage(),e);
	}
    }
    
    public static Configuration fromXml(String xml) {
	Configuration conf = new Configuration();
	
	InputStream inputStream = IOUtils.toInputStream(xml);
	conf.addResource(inputStream);
	IOUtils.closeQuietly(inputStream);
	
	return conf;
    }
}
