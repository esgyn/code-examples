//package com.esgyn.kafka;

//@@@ START COPYRIGHT @@@
//
//Copyright (c) 2017, Esgyn Corporation, http://www.esgyn.com.
//
//Licensed under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//
//@@@ END COPYRIGHT @@@

//This is a UDR that can be used to read from a Kafka topic.
//The UDR is invoked like this:
//
//select * from udf(kafka('<broker connection string',
//	                    '<group id>',
//                      '<topic>',
//                      <topic partition number>,
//                      '<starting timestamp>',
//                      '<output column descriptions>',
//                      '<field delimiter>',
//                      <max number of rows to read>,
//                      <stream timeout in milliseconds> ));
//
//Required arguments:
//
//<broker connection string>: Info to connect to kafka broker.
//                             Example: 'host.somedomain.com:9092'
//<group id>:                    Kafka group id. Example: 'beatles'
//<topic>:                       Name of Kafka topic.
//
//<topic partition number>:		 topic partition to read from, or 0
//<starting timestamp>:      	 format: yyyy-mm-dd hh:mm:ss 
//
//Optional arguments:
//
//<output column descriptions>:  A list of characters, one for every
//                             delimited field in the Kafka messages:
//                               Cnnnn  Character field with nnnn characters
//                               D      Date field (2015-12-21)
//                               F      Floating point field (3.14E0 or 3.14)
//                               I      Integer field
//                               L      Long field
//                               Npp.ss Numeric field with precision pp and scale ss
//                               Snn    Timestamp field (yyyy-mm-dd hh:mm:ss.ffffff)
//                                      with fraction precision nn (0-6)
//                               T      Time field (hh:mm:ss)
//                             Examples:
//                             'IIC20N18.2' (integer, integer, char(20 bytes), numeric(18,2))
//                             Default: '' (single text field, 10,000 chars, no delimiters)
//<field delimiter>:              The UDF assumes to get a delimited
//                             record of one of more columns, delimited
//                             by this single character.
//                             Examples: ' ' or '|' or ','
//                             Default: ',' (delimiters in fields can be quoted,
//                                           using double quotes)
//<max number of rows to read>:  The UDF will stop after reading this number
//                             of rows
//                             Default: -1 (means read an unlimited number)
//<stream timeout in millisec>:  Stop after n milliseconds of waiting for
//                             a message
//                             Default: 60000 (1 minute)
//The UDF is created with this DDL:
		/*
		create table_mapping function kafka()
			external name 'com.esgyn.kafka.KafkaSerialConsumer2'
			language java
			library <name of library>;
		*/
//
//The UDF will connect to a Kafka broker specified by brokerString.
//It will then read up to <maxRows> rows from topic <topic> and
//it will return if there is no activity for <stream timeout>
//milliseconds.
//
//To name the output columns, use a correlation name:
//
//select * from udf(kafka(...)) as T(col1, col2, ....)

import org.trafodion.sql.udr.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class UdfConsumer extends UDR {

 public UdfConsumer() {}
	
 static class inputParams {
     public String  connectionString_;
     public String  groupId_;
     public String  topic_;
     public String  outputColDescriptions_ = "";
     public char    fieldDelim_ = ',';
     public long    numRowsToRead_ = -1;
     public int     streamTimeout_ = -1;
     public String  timestamp_ = "";
     public int     partition_ = -1;
	
     inputParams(
                 String connectionString,
                 String groupId,
                 String topic,
                 String outputColDescriptions,
                 char fieldDelim,
                 long numRowsToRead,
                 int streamTimeout,
                 int partition,
                 String  timestamp)
     {
         connectionString_ = connectionString;
         groupId_ = groupId;
         topic_ = topic;
         outputColDescriptions_ = outputColDescriptions;
         fieldDelim_ = fieldDelim;
         numRowsToRead_ = numRowsToRead;
         streamTimeout_ = streamTimeout;
         timestamp_ = timestamp;
         partition_ = partition;
     }
 }
	
 static class typeDecoder {
     public String colEncodings_;
     int len_ = 0;
     int pos_ = 0;

     typeDecoder(String colEncodings) {
         colEncodings_ = colEncodings;
         len_ = colEncodings_.length();
     }

     int decodeNumber()
     {
         int result = 0;
         while (pos_ < len_ &&
                colEncodings_.charAt(pos_) >= '0' &&
                colEncodings_.charAt(pos_) <= '9')
             result = 10*result + colEncodings_.charAt(pos_++) - '0';
         return result;
     }

     TypeInfo getNextType() throws UDRException {
         if (pos_ < len_)
	        {
                 TypeInfo.SQLTypeCode typeCode = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;
                 int length = 0;
                 boolean nullable = true;
                 int scale = 0;
                 TypeInfo.SQLCharsetCode charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
                 TypeInfo.SQLIntervalCode intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
                 int precision = 0;

                 switch (colEncodings_.charAt(pos_++))
	        	{
	        	case 'C':
                         typeCode = TypeInfo.SQLTypeCode.CHAR;
                         length = decodeNumber();
                         break;
	        	case 'D':
                         typeCode = TypeInfo.SQLTypeCode.DATE;
                         break;
	        	case 'F':
                         typeCode = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
                         break;
	        	case 'I':
                         typeCode = TypeInfo.SQLTypeCode.INT;
                         break;
	        	case 'L':
                         typeCode = TypeInfo.SQLTypeCode.LARGEINT;
                         break;
	        	case 'N':
                         typeCode = TypeInfo.SQLTypeCode.NUMERIC;
                         precision = decodeNumber();
                         if (pos_ < len_ && colEncodings_.charAt(pos_) == '.')
	        		{
                                 pos_++;
                                 scale = decodeNumber();
	        		}
                         break;
	        	case 'S':
                         typeCode = TypeInfo.SQLTypeCode.TIMESTAMP;
                         scale = decodeNumber();
                         break;
	        	case 'T':
                         typeCode = TypeInfo.SQLTypeCode.TIME;
                         break;
	        	default:
                         throw new UDRException(
                                                38001,
                                                "Expecting 'C', 'D', 'F', 'I', 'L', 'N', 'S', or 'T' in fourth argument, got %s",
                                                colEncodings_.substring(pos_-1));
	        	}

                 return new TypeInfo(
	        			typeCode,
	        			length,
	        			nullable,
	        			scale,
	        			charset,
	        			intervalCode,
	        			precision);
	        }
	        else
                 return null;
     }
 }
	
 static KafkaConsumer<String,String> getConsumer(
                                        String brokerString,
                                        String groupId,
                                        String topic,
                                        int streamTimeout) {
     Properties props = new Properties();
     props.put("bootstrap.servers", brokerString);
     props.put("group.id", groupId);
     props.put("session.timeout.ms", "10000");			// zookeeper
     props.put("auto.commit.interval.ms", "1000");
     props.put("client.id", "consumer_" + topic);
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // props.put("auto.offset.reset", "smallest");

     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

     return consumer;
 }
	

 inputParams getInputParams(UDRInvocationInfo info) throws UDRException {
     final int MIN_REQD_PARAMS = 4;
	 String  connectionString = null;
     String  groupId = null;
     String  topic = null;
     String  outputColDescriptions = "";
     char    fieldDelim = ',';
     long    numRowsToRead = -1;
     int     streamTimeout = 60000;
     int     partition = -1;
     String  timestamp = null;
	
     int numInputParams = info.par().getNumColumns();

     if (info.getCallPhase() == UDRInvocationInfo.CallPhase.COMPILER_INITIAL_CALL)
         {
             if (numInputParams < MIN_REQD_PARAMS)
                 throw new UDRException(
                                        38010,
                                        "Expecting at least " + MIN_REQD_PARAMS + " parameters.");
             // required
             if (info.par().getColumn(0).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                 throw new UDRException(
                                        38020,
                                        "Expecting a character string as first argument.");
             if (info.par().getColumn(1).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                 throw new UDRException(
                                        38030,
                                        "Expecting a character string as second argument.");
             if (info.par().getColumn(2).getType().getSQLTypeClass() !=
                     TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                     throw new UDRException(
                                        38040,
                                        "Expecting a character string as third argument.");
             if (info.par().getColumn(3).getType().getSQLTypeClass() !=
                     TypeInfo.SQLTypeClassCode.NUMERIC_TYPE)
                     throw new UDRException(
                                        38050,
                                        "Expecting a number (partition) as fourth argument.");
             // below are optional
             if (info.par().getColumn(4).getType().getSQLTypeClass() !=
                     TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                     throw new UDRException(
                                        38060,
                                        "Expecting a timestamp (yyyy-mm-dd hh:mm:ss) as fifth argument.");
             if (numInputParams >= 5 &&
                 info.par().getColumn(5).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                 throw new UDRException(
                                        38070,
                                        "Expecting a character string as sixth argument, if specified.");
             if (numInputParams >= 6 &&
                 info.par().getColumn(6).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.CHARACTER_TYPE)
                 throw new UDRException(
                                        38080,
                                        "Expecting a character string as seventh argument, if specified.");
             if (numInputParams >= 7 &&
                 info.par().getColumn(7).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.NUMERIC_TYPE)
                 throw new UDRException(
                                        38090,
                                        "Expecting a number as eighth argument, if specified.");
             if (numInputParams >= 8 &&
                 info.par().getColumn(8).getType().getSQLTypeClass() !=
                 TypeInfo.SQLTypeClassCode.NUMERIC_TYPE)
                 throw new UDRException(
                                        38090,
                                        "Expecting a number as ninth argument, if specified.");
         }

     // assign input parameters to local variables
     for (int i=0; i<numInputParams; i++)
         {
             if (info.par().isAvailable(i))
                 {
                     switch (i)
                         {
                         case 0:
                             connectionString = info.par().getString(i);
                             break;
                         case 1:
                             groupId = info.par().getString(i);
                             break;
                         case 2:
                             topic = info.par().getString(i);
                             break;
                         case 3:
                        	 partition = info.par().getInt(i);
                             break;
                         case 4:
                        	 timestamp = info.par().getString(i);
                             break;
                         case 5:
                             outputColDescriptions = info.par().getString(i);
                             break;
                         case 6:
                             fieldDelim = info.par().getString(i).charAt(0);
                             break;
                         case 7:
                             numRowsToRead = info.par().getInt(i);
                        	 break;
                         case 8:
                             streamTimeout = info.par().getInt(i);
                        	 break;
                         case 9:
                             throw new UDRException(
                                                    38070,
                                                    "More than 9 arguments provided.");
                         }
                 }
             else
                 throw new UDRException(
                                        38080,
                                        "Parameter number %d must be a literal.",
                                        i);

             if (info.getCallPhase() == UDRInvocationInfo.CallPhase.COMPILER_INITIAL_CALL)
                 // we need to make the formal parameter list match
                 // the actual parameters
                 info.addFormalParameter(info.par().getColumn(i));
         }

     return new inputParams(
                            connectionString,
                            groupId,
                            topic,
                            outputColDescriptions,
                            fieldDelim,
                            numRowsToRead,
                            streamTimeout,
                            partition,
                            timestamp);
 }

 @Override
 public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
     // input parameter values
     inputParams in = getInputParams(info);
     typeDecoder tDec = new typeDecoder(in.outputColDescriptions_);
     TypeInfo ty = null;
     int colNum = 0;

     while ((ty = tDec.getNextType()) != null)
         {
             String colName = "COL" + String.valueOf(colNum++);
             info.out().addColumn(
                                  new ColumnInfo(colName, ty));
         }

     if (info.out().getNumColumns() == 0)
         // no output column descriptions provided, add a single char column
         info.out().addCharColumn("MESSAGE", 10000, true);
 }

 // the actual UDF code that gets invoked at runtime
 @Override
 public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {
     KafkaConsumer<String,String> cc = null;

     // get parameters
     inputParams in = getInputParams(info);
     long offsetOfTs = -1;
     
     try {
         cc = getConsumer(in.connectionString_,
                            in.groupId_,
                            in.topic_,
                            in.streamTimeout_);
         
         TopicPartition desiredPartition = new TopicPartition(in.topic_, in.partition_);
         // udf called with timestamp position
         if (!in.timestamp_.equalsIgnoreCase("null")) {
		         // timestamp -> epoch form
		         long uxTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(in.timestamp_).getTime();
		         // designate partition to use
		         Map<TopicPartition, Long> pTsMap = new HashMap<TopicPartition, Long>();
		         pTsMap.put(desiredPartition, uxTime);
		         // find offset corresponding to the timestamp
		         Map<TopicPartition,OffsetAndTimestamp> offsetResults = cc.offsetsForTimes(pTsMap);
		         if (!offsetResults.isEmpty()) {
		        	// for 1 partition can only be 1
		        	 for (OffsetAndTimestamp o : offsetResults.values() ) {
		        		 offsetOfTs = o.offset();
		        	 }
		         } // if 
		         else
		        	 throw new UDRException (38199, "No offset associated with this timestamp");
		         
		         cc.assign(Arrays.asList(desiredPartition));
		         // position
		         cc.seek(desiredPartition, offsetOfTs);
         }  // if null TS
         else {
        	 // position to beginning of partition
 			cc.subscribe(Arrays.asList(in.topic_));
 			// priming poll
 			cc.poll(100);
 			// start from beginning
 			cc.seekToBeginning(	Arrays.asList(desiredPartition) );
        	 
         }
         long numRows = 0;
         
     	 while (in.numRowsToRead_ < 0 || in.numRowsToRead_ > numRows) {
    		 ConsumerRecords<String, String> records = cc.poll(in.streamTimeout_);
    		 if (records.isEmpty()) break;               // timed out
    		 for (ConsumerRecord<String, String> msg : records) {
                 info.out().setFromDelimitedRow(
            		 						msg.value(),    // delimited row
                                            in.fieldDelim_, // field delimiter
                                            true,           // allow quoted strings
                                            '"',            // quote symbol
                                            0,              // first field to set
                                            -1,             // last field to set
                                            0);             // fields to skip
                 emitRow(info);
                 numRows++;
    		 } // for
         } // while
     }
     catch (UDRException e) {
         throw e;
     }
     catch (Exception e) {
         throw new UDRException(38000,
                                "Exception from Kafka: " + e.getMessage());
     }
     finally {
         if (cc != null)
             cc.close();
     }
 }

 // for standalone testing, but usually this is invoked as a UDR
 public static void main(String[] args) {
     String brokerString = "localhost:9092";
     String groupId = "group5";
     String topic = null;
     int partition = 1;
     long maxRows = -1;
     long offsetOfTs = -1;
     int streamTimeout = 60000;
     String timestamp = null;
     
     topic = args[0];
     partition = Integer.parseInt(args[1]);
     timestamp = args[2];
     maxRows = Integer.parseInt(args[3]);
     System.out.println("topic=" + topic + "; partition=" + args[1] + "; ts=" + args[2] + "; msgs=" + args[3]);
     
     try {
    	 KafkaConsumer<String,String> cc = getConsumer(
                                              brokerString,
                                              groupId,
                                              topic,
                                              streamTimeout);

    	 if (!timestamp.equalsIgnoreCase("NULL")) {
		    	 // convert input timestamp to epoch form, then find the correspoinding offset
		    	 // in this partition
		         // timestamp -> epoch form
		         long uxTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp).getTime();
		         // designate partition to use
		         TopicPartition desiredPartition = new TopicPartition(topic, partition);
		         Map<TopicPartition, Long> pTsMap = new HashMap<TopicPartition, Long>();
		         pTsMap.put(desiredPartition, uxTime);
		         // find offset corresponding to the timestamp
		         Map<TopicPartition,OffsetAndTimestamp> offsetResults = cc.offsetsForTimes(pTsMap);
		         if (!offsetResults.isEmpty()) {
		        	// for 1 partition can only be 1
		        	 for (OffsetAndTimestamp o : offsetResults.values() ) {
		        		 offsetOfTs = o.offset();
		        	 }
		         } // if 
		         else
		        	 throw new UDRException (38199, "No offset associated with this timestamp");
		         
		         cc.assign(Arrays.asList(desiredPartition));
		         // position
		         cc.seek(desiredPartition, offsetOfTs);
    	 } // not null TS
    	 else {
        	 // position to beginning of partition
 			cc.subscribe(Arrays.asList(topic));
 			// priming poll
 			cc.poll(100);
 			// start from beginning
 			cc.seekToBeginning(	Arrays.asList(new TopicPartition(topic, partition)) );
         }

    		 
         long numRows = 0;

         while (maxRows < 0 || maxRows > numRows) {
    		 ConsumerRecords<String, String> records = cc.poll(streamTimeout);
        	 System.out.println("poll return " + records.count());
    		 if (records.isEmpty()) break;               // timed out
    		 for (ConsumerRecord<String, String> msg : records) {
    			 String msgTS = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date (msg.timestamp()));
                 System.out.println("TS=" + msgTS + " Value=" + msg.value());
                 numRows++;
             }
         } // while
         cc.close();
     }
     catch (Exception e) {
         System.out.println("Exception during main(): " + e.getMessage());
     }
 }
}