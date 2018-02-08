//package com.esgyn.kafka;
import org.apache.commons.cli.DefaultParser;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
//import java.util.Set;
import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.trafodion.jdbc.t2.*;

import kafka.consumer.ConsumerTimeoutException;
@SuppressWarnings("deprecation") 		

//@@@ START COPYRIGHT @@@
//
//Copyright (c) 2016, Esgyn Corporation, http://www.esgyn.com.
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

/*
 * This represents a typical Kafka consumer that uses EsgynDB for data storage.
 * It is a single-threaded server that can be replicated to scale out - each copy 
 * handling a partition of a topic.
 * 
 * Execution of each server in the group handled by...
 *   TBD
 *   a.  pdsh script per node
 *   b.  zookeeper?
 *   
 *   
 */

// flow:
//   get execution parameters
//   make topic connection - which partition is assigned?
//   check status table in DB for where this partition left off
//   set to start from this offset
//   poll...

//   if no restart info in status table, start at end, poll...

//   while polling returns records
//		upsert into esgyndb table
//      kafka commit every <param value> record
//		update offset value into status table for this topic/partition

/*
 *  Added option to insert using UDF.
 *  
 *  UDF runs until "no more records"...in Kafka case, we'd want UDF to exit after 'X' number of records
 *  or some T/O value of msg reception...so that we can re-drive the call.
 *  In this example, we have a pile of messages already in the topic and not an active pub-sub situation.
 *  It's been set up to demonstrate the performance difference of INSERTing via "normal" row inserts versus
 *  inserts directly from UDF output.
 *  
 *  In practice, the UDF would return context info that allows a subsequent call to the UDF to pickup where
 *  the previous left off.  The consumer would need to safe store / transction protect this info for restart /
 *  recovery purposes.
 *  
 */
public class ConsumerServer {
	
	private final long DEFAULT_STREAM_TO_MS = 20000;
	private final long DEFAULT_ZOOK_TO_MS = 10000;
	private final long DEFAULT_COMMIT_COUNT = 500;
	private PreparedStatement pStmt;
	private Connection conn;
	
	// execution settings
	String zookeeper; 
	String broker;
	String topic;
	String groupID;
	String columnCodes;
	long   streamTO;
	long   zkTO;
	long   commitCount;
	char   delimiter;
	int    udfMode;
	int    insMode;
	boolean autoCommit;
	boolean t2Connect;
	
	KafkaConsumer<String, String> kafka;
	
	ConsumerServer() {
		zookeeper = ""; 
		broker = "";
		topic = "";
		groupID = "";
		columnCodes = "";
		streamTO = 0;
		zkTO = 0;
		commitCount = 0;
		delimiter = ',';
		udfMode = -1;
		autoCommit = false;
		insMode = -1;
		t2Connect = false;
	}
	
	
	public void init( String [] args ) throws ParseException {
		/*
		 * Get command line args
		 * 
		 * Cmd line params:
		 * -z --zk		zookeeper connection (node:port[/kafka?]
         * -b --broker	broker location
         * -t --topic	topic
         * -g --group	groupID
         * --sto		stream T/O (data polling)
         * --zkto		zk T/O 
         * --cols		output column descriptions - Hans' code
         * -d --delim	field delimiter - Hans' code
         * -c --commit	commit interval (num recs)
         * -u --udf     use insert via udf
         *    --autocom autocommit
         * -i --insert  iterative insert/upsert
         *    --t2      use T2 JDBC, defaults to T4
         *
		 */
		
		Option zkOption = Option.builder("z")
				  .longOpt("zook")
				  .required(false)
				  .hasArg()
				  .desc("zookeeper connection list, ex: <node>:port[/kafka],...")
				  .build();
		Option brokerOption = Option.builder("b")
				  .longOpt("broker")
				  .required(false)
				  .hasArg()
				  .desc("bootstrap.servers setting, ex: <node>:9092")
				  .build();
		Option topicOption = Option.builder("t")
				  .longOpt("topic")
				  .required(true)
				  .hasArg()
				  .desc("REQUIRED. topic of subscription")
				  .build();
		Option groupOption = Option.builder("g")
				  .longOpt("group")
				  .required(true)
				  .hasArg()
				  .desc("REQUIRED. groupID for this consumer")
				  .build();
		Option stoOption = Option.builder()
				  .longOpt("sto")
				  .required(false)
				  .hasArg()
				  .desc("kafka poll time-out limit, default 60000ms")
				  .build();
		Option ztoOption = Option.builder()
				  .longOpt("zkto")
				  .required(false)
				  .hasArg()
				  .desc("zookeeper time-out limit, default 10000ms")
				  .build();
		Option colsOption = Option.builder()
				  .longOpt("cols")
				  .required(false)
				  .hasArg()
				  .desc("encode of output column defs, one for each delimited field in Kafka message")
				  .build();
		Option delimOption = Option.builder("d")
				  .longOpt("delim")
				  .required(false)
				  .hasArg()
				  .desc("field delimiter, default: ','(comma)")
				  .build();
		Option commitOption = Option.builder("c")
				  .longOpt("commit")
				  .required(false)
				  .hasArg()
				  .desc("num message per Kakfa synch, default: 500")
				  .build();
		Option t2Option = Option.builder()
				  .longOpt("t2")
				  .required(false)
				  .desc("use T2 JDBC, default: use T4")
				  .build();
		Option udfOption = Option.builder("u")
				  .longOpt("udf")
				  .required(false)
				  .hasArg()
				  .desc("use UDF for insert: 1 = upsert load; 2 = insert")
				  .build();
		Option autocOption = Option.builder()
				  .longOpt("autocom")
				  .required(false)
				  .desc("autocommit, default false")
				  .build();
		Option insOption = Option.builder("i")
				  .longOpt("insert")
				  .required(false)
				  .hasArg()
				  .desc("insert mode: 1 = insert; 2 = upsert")
				  .build();
		
		Options exeOptions = new Options();
		exeOptions.addOption(zkOption);
		exeOptions.addOption(brokerOption);
		exeOptions.addOption(topicOption);
		exeOptions.addOption(groupOption);
		exeOptions.addOption(stoOption);
		exeOptions.addOption(ztoOption);
		exeOptions.addOption(colsOption);
		exeOptions.addOption(delimOption);
		exeOptions.addOption(commitOption);
		exeOptions.addOption(udfOption);
		exeOptions.addOption(autocOption);
		exeOptions.addOption(insOption);
		exeOptions.addOption(t2Option);
		
		
		// With required options, can't have HELP option to display help as it will only 
		// indicate that "required options are missing"
		if (args.length == 0) {
		    HelpFormatter formatter = new HelpFormatter();
		    formatter.printHelp("Consumer Server", exeOptions);
		    System.exit(0);
		}
	     
		CommandLineParser parser = new DefaultParser();
		CommandLine cmdLine = parser.parse(exeOptions, args);

		// for the required options, move the value
		topic = cmdLine.getOptionValue("topic");
		groupID = cmdLine.getOptionValue("group");
		zookeeper = cmdLine.hasOption("zook") ? cmdLine.getOptionValue("zook") : null; 
		broker = cmdLine.hasOption("broker") ? cmdLine.getOptionValue("broker") : null;
		columnCodes = cmdLine.hasOption("cols") ? cmdLine.getOptionValue("cols") : "";
		streamTO = cmdLine.hasOption("sto") ? Long.parseLong(cmdLine.getOptionValue("sto")) : DEFAULT_STREAM_TO_MS;
		zkTO = cmdLine.hasOption("zkto") ? Long.parseLong(cmdLine.getOptionValue("zkto")) : DEFAULT_ZOOK_TO_MS;
		commitCount = cmdLine.hasOption("commit") ? Long.parseLong(cmdLine.getOptionValue("commit")) : DEFAULT_COMMIT_COUNT;
		delimiter = cmdLine.hasOption("delim") ? cmdLine.getOptionValue("delim").charAt(0) : ',';
		udfMode = cmdLine.hasOption("udf") ? Integer.parseInt(cmdLine.getOptionValue("udf")) : 0;
		autoCommit = cmdLine.hasOption("autocom") ? true : false;
		insMode = cmdLine.hasOption("insert") ? Integer.parseInt(cmdLine.getOptionValue("insert")) : 0;
		t2Connect = cmdLine.hasOption("t2") ? true : false;
		
		// one of zook | broker must be given
		if ( zookeeper == null && broker == null ) {
			System.out.println ("*** Error: Must provide zookeeper or broker string" );
			System.exit(0);
		}
	}

	public void getConsumer(String zkOrBroker, boolean useZk) {
		/*
		 * instantiate a KafkaConsumer with the cmd line options
		 * 
		 * Older versions of Kafka might only recognize ZK interface - allow for that
		 * 
		 */
	     Properties props = new Properties();
	     
 	     // props.put("auto.offset.reset", "smallest");
	     if ( useZk ) props.put("zookeeper.connect", zkOrBroker);
	     else
	    	 props.put("bootstrap.servers", zkOrBroker);
	     props.put("group.id", groupID);
	     props.put("enable.auto.commit", "false");			
	     props.put("session.timeout.ms", String.valueOf(zkTO));				// zookeeper wait t/o
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     kafka = new KafkaConsumer<>(props);
	     
	}
	
	public void processMessages() {
		// for this exercise start from offset 0
		// produce batches of n size for jdbc and insert
		
		// for this table
		// char(10), char(20), long
		String sqlInsert = "INSERT INTO kblog.BLOGDATA VALUES (?,?,?,?,?)";
		String sqlUpsert = "UPSERT INTO kblog.BLOGDATA VALUES (?,?,?,?,?)";
		final String UDFCALL = 	 " select * from udf(kblog.kaf3('nap007:9092'," + 
								 " 'gid'," + 
								 " 'blogit'," + 
								 "  0," + 
								 " 'null'," + 
								 " 'C10C20IC55C55'," + 
								 " '|'," + 
								 "  -1," + 
								 "  1000 ))" ;
		final String SQLUPSERT = "upsert using load into kblog.blogdata " ;
		final String SQLINSERT = "insert into kblog.blogdata " ;
		final String SQLUPSERT2 = "upsert into kblog.blogdata ";
		
		try {
				if (t2Connect) {
					// T2
					Class.forName( "org.apache.trafodion.jdbc.t2.T2Driver" ) ; 
					conn = DriverManager.getConnection( "jdbc:t2jdbc:" ) ;
				}
				else {
					// T4
					Class.forName( "org.trafodion.jdbc.t4.T4Driver" ) ; 
					conn = DriverManager.getConnection( "jdbc:t4jdbc://nap007:23400/:", "trafodion", "passw" ) ;
				}
				conn.setAutoCommit(autoCommit);
		}
		catch (SQLException sx) {
			System.out.println ("SQL error: " + sx.getMessage());
			System.exit(1);
		}
		catch (ClassNotFoundException cx) {
			System.out.println ("Driver class not found: " + cx.getMessage());
			System.exit(2);
			
		}
		
		// message processing loop
		String[] msgFields;
		long numRows = 0;
		long totalRows = 0;
		int[] batchResult;
		
		if ( udfMode == 0 && insMode == 0 ) {
			// missing cmd line setting
			System.out.println("*** Neither UDF nor INSERT mode specified - aborting ***");
			System.exit(2);
		}
		
		try {
				if (udfMode > 0 ) {
					long diff = 0;
					
					long startTime = System.currentTimeMillis();
					switch (udfMode) {
						case 1:			// upsert using load
							pStmt = conn.prepareStatement(SQLUPSERT + UDFCALL);
							totalRows = pStmt.executeUpdate();
							diff = (System.currentTimeMillis() - startTime );
							System.out.println("Upsert loaded row count: " + totalRows + " in " + diff + " ms");
							break;
							
						case 2:			// insert 
							pStmt = conn.prepareStatement(SQLINSERT + UDFCALL);
							totalRows = pStmt.executeUpdate();
							if (!autoCommit) {
								conn.commit();
								diff = (System.currentTimeMillis() - startTime );
								System.out.println("Insert row count (autocommit off): " + totalRows + " in " + diff + " ms");
							}
							else {							
								diff = (System.currentTimeMillis() - startTime );
								System.out.println("Insert row count (autocommit on): " + totalRows + " in " + diff + " ms");
							}
							break;
							
						case 3:			// upsert 
							pStmt = conn.prepareStatement(SQLUPSERT2 + UDFCALL);
							totalRows = pStmt.executeUpdate();
							if (!autoCommit) {
								conn.commit();
								diff = (System.currentTimeMillis() - startTime );
								System.out.println("Upsert row count (autocommit off): " + totalRows + " in " + diff + " ms");
							}
							else {							
								diff = (System.currentTimeMillis() - startTime );
								System.out.println("Upsert row count (autocommit on): " + totalRows + " in " + diff + " ms");
							}
							break;
							
						default:   // illegal value
							System.out.println("*** Only udf values 1,2,3 allowed; found: " + udfMode);
							System.exit(2);
							
					} // switch
				
				} // udfMode
			else  { // iterative insert/upsert
				
				    switch (insMode) {
				    	case 1:  // insert
							pStmt = conn.prepareStatement(sqlInsert);
							break;
				    	case 2:  //upsert
							pStmt = conn.prepareStatement(sqlUpsert);
							break;
						default:  // illegal
							System.out.println("*** Only insert values 1,2 allowed; found: " + insMode);
							System.exit(2);
				    } // switch
				    
					kafka.subscribe(Arrays.asList(topic));
					// priming poll
					kafka.poll(100);
					// always start from beginning
					kafka.seekToBeginning(
							Arrays.asList(new TopicPartition(topic, 0))
							);
					
				// enable autocommit and singleton inserts for comparative timings
					
					long startTime = System.currentTimeMillis();
					while(true) {
						// note that we don't commitSync to kafka - tho we should
						ConsumerRecords<String, String> records = kafka.poll(streamTO);
			    		if (records.isEmpty()) break;               // timed out
			    		for (ConsumerRecord<String, String> msg : records) {
			    			msgFields = msg.value().split("\\"+Character.toString(delimiter));
			    			
			    			// position info for this message
			    			long offset = msg.offset();
			    			int  partition = msg.partition();
			    			String topic = msg.topic();
			    			
			    			pStmt.setString(1, msgFields[0]);
			    			pStmt.setString(2, msgFields[1]);
			    			pStmt.setLong(3, Long.parseLong(msgFields[2]));
			    			pStmt.setString(4, msgFields[3]);
			    			pStmt.setString(5, msgFields[4]);
			                numRows++;
			                totalRows++;
			                if (autoCommit) {
			                	// single ins/up sert
			                	pStmt.executeUpdate();
			                }
			                else {
				                pStmt.addBatch();
				                if ((numRows % commitCount) == 0) {
				                	numRows=0;
				                	batchResult = pStmt.executeBatch();
				                	conn.commit();
				                }
			                }
			                
			    		 } // for each msg
		
					} // while true
					
					// get here when poll returns no records
					if (numRows > 0 && !autoCommit) {
						// remaining rows
			        	batchResult = pStmt.executeBatch();
			        	conn.commit();
					}
					long diff = (System.currentTimeMillis() - startTime );
					if (autoCommit)
						System.out.println("Total rows: " + totalRows + " in " + diff + " ms");
					else
						System.out.println("Total rows: " + totalRows + " in " + diff + " ms; batch size = " + commitCount);
					
					kafka.close();
			} // else
				
		} // try
			catch (ConsumerTimeoutException to) {
				System.out.println("consumer time out; " + to.getMessage());
				System.exit(1);
			}
			catch (BatchUpdateException bx) {
				int[] insertCounts = bx.getUpdateCounts();
				int count = 1;
				for (int i : insertCounts) {
					if ( i == Statement.EXECUTE_FAILED ) 
						System.out.println("Error on request #" + count +": Execute failed");
					else count++;
				}
				System.out.println(bx.getMessage());
				System.exit(1);
				
			}
			catch (SQLException sx) {
				System.out.println ("SQL error: " + sx.getMessage());
				System.exit(1);
			}
	
	}
	
	
	public static void main(String[] args) {
		
		ConsumerServer me = new ConsumerServer();
		
		try {
			me.init(args);
		}
		catch (ParseException p) {
			System.out.println ("Cmd line error: " + p.getMessage());
			System.exit (0);
		}
		// debug
				System.out.println( "zookeeper = " +  me.zookeeper); 
				System.out.println( "broker = " + me.broker);
				System.out.println( "topic = " + me.topic);
				System.out.println( "groupID = " + me.groupID);
				System.out.println( "columnCodes = " + me.columnCodes);
				System.out.println( "streamTO = " + me.streamTO);
				System.out.println( "zkTO = " + me.zkTO);
				System.out.println( "commitCount = " + me.commitCount);
				System.out.println( "delimiter = " + me.delimiter);
				System.out.println( "udfMode = " + me.udfMode);
				System.out.println( "insMode = " + me.insMode);
				System.out.println( "autocommit = " + me.autoCommit);
				System.out.println( "t2Driver = " + me.t2Connect);
			

		// connect to kafka w/ either broker/zook setting
		if (me.zookeeper == null) me.getConsumer(me.broker, false);
		else me.getConsumer(me.zookeeper, true);
		
		me.processMessages();

	}

}

