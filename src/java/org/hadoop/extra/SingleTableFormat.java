package org.hadoop.extra;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleTableFormat extends TableInputFormat {

    
    public boolean isTable(byte[] tableName) {
	return Bytes.equals(this.getHTable().getTableName(), tableName);
    }
}
