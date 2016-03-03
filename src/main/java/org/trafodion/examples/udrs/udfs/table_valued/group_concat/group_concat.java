// @@@ START COPYRIGHT @@@
//
// Copyright (c) 2016, Esgyn Corporation, http://www.esgyn.com.
//
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.examples.udrs.udfs.table_valued.group_concat;

import org.trafodion.sql.udr.*;

/*********************************************************************
 * This UDF groups rows by one column and concatenates the values
 * of one column occurring in the group. It behaves like an aggregate
 * or sequence function, but rather than computing min or max, etc.
 * it computes a concatenation of the values in the group.
 *
 * Here is how to invoke the UDF:
 *
   select *
   from udf(group_concat(table(<query>
                               partition by <group column>
                               order by <concatenation column>),
                               <max. length of concatenated string>,
                               '<separator>'));
 *  
 * Example:
 *
   select *
   from udf(group_concat(table(select *
                               from (values (1,1),
                                            (1,2),
                                            (2,0)) T(a,b)
                               partition by a
                               order by b),
                               10,
                               ','));
 *
 * Output:
 *
 * A       B_CONCAT  
 * ------  ----------
 *
 *      1  1,2       
 *      2  0         
 *
 * Here is how to create the UDF:

   drop function group_concat;
   drop library group_concat_lib;
   create library group_concat_lib
      file '<directory>/group_concat.jar';
   create table_mapping function group_concat(
      output_string_length integer,
      delimiter char(10))
   external name
    'org.trafodion.examples.udrs.udfs.table_valued.group_concat.group_concat'
   language java
   library group_concat_lib;

 **************************************************************************/

class group_concat extends UDR {

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info)
   throws UDRException
  {
    // First, validate PARTITION BY and ORDER BY columns
    // Make sure we have exactly one table-valued input, otherwise
    // generate a compile error
    if (info.getNumTableInputs() != 1)
      throw new UDRException(
          38000,
          "%s must be called with one table-valued input",
          info.getUDRName());
    // check whether there is a PARTITION BY for the
    // input table that specifies the single
    // partitioning column we support
    PartitionInfo queryPartInfo =
          info.in().getQueryPartitioning();
    if (queryPartInfo.getType() != PartitionInfo.PartitionTypeCode.PARTITION ||
        queryPartInfo.getNumEntries() != 1)
      throw new UDRException(
          38001,
          "Expecting a PARTITION BY clause with a single column for the input table.");
    // check whether there is an ORDER BY for the
    // input table, indicating the column to be concatenated
    OrderInfo queryOrderInfo =
          info.in().getQueryOrdering();
    if (queryOrderInfo.getNumEntries() != 1)
      throw new UDRException(
          38002,
          "Expecting an ORDER BY with a single column for the input table, indicating the column to be concatenated.");
    // Check whether the first scalar parameter (output string length)
    // is available
    if (!info.par().isAvailable(0))
      throw new UDRException(
          38003,
          "First scalar parameter of %s (output string length) must be a constant.");

    // Second, define the output parameters:
    // The partitioning column of the input table will be returned
    ColumnInfo partCol = info.in().getColumn(queryPartInfo.getColumnNum(0));
    info.out().addColumn(new ColumnInfo(partCol));
    
    // A string column with the concatenated values will be returned,
    // Use the same character set as the source column. The length
    // of the varchar column is specified as the first scalar parameter.
    ColumnInfo concatCol = info.in().getColumn(queryOrderInfo.getColumnNum(0));
    TypeInfo concatSrcType = concatCol.getType();
    int concatStringLength = info.par().getInt(0);
    TypeInfo.SQLCharsetCode concatCharSet = concatSrcType.getCharset();

    // if the source type doesn't have a character set, choose some
    // reasonable default
    if (concatCharSet == TypeInfo.SQLCharsetCode.UNDEFINED_CHARSET)
      concatCharSet = TypeInfo.SQLCharsetCode.CHARSET_UTF8;

    info.out().addColumn(
        new ColumnInfo(concatCol.getColName()+"_CONCAT",
        new TypeInfo(
            TypeInfo.SQLTypeCode.VARCHAR,
            concatStringLength,
            false, // nulls will be considered empty strings
            0,
            concatCharSet,
            TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE,
            0,
            TypeInfo.SQLCollationCode.SYSTEM_COLLATION)));
   
    // Set the function type, group_concat behaves like
    // a reducer in MapReduce, it does not carry state
    // between partitions. Doing this allows the UDF to
    // execute in parallel.
    info.setFuncType(UDRInvocationInfo.FuncType.REDUCER);
  }
  
  @Override
  public void describeStatistics(UDRInvocationInfo info)
          throws UDRException
  {
    // this UDF returns one output row for each partition
    info.out().setEstimatedNumRows(info.in().getEstimatedNumPartitions());
  }
  
  @Override
  public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {

    int partColNum = info.in().getQueryPartitioning().getColumnNum(0);
    int concatColNum = info.in().getQueryOrdering().getColumnNum(0);
    String lastPartVal = "";
    String concatResult = "";
    String delim = info.par().getString(1);
    boolean initialState = true;

    // loop over input rows
    while (getNextRow(info))
    {
      String currPartVal = info.in().getString(partColNum);
      String concatToken = info.in().getString(concatColNum);
      
      if (lastPartVal.equals(currPartVal) &&
          !initialState)
      {
        // an additional row in a partition, add a delimiter, if needed
        if (!concatResult.isEmpty() && !concatToken.isEmpty())
          concatResult = concatResult.concat(delim);
        concatResult = concatResult.concat(concatToken);
      }
      else
      {
        // a new partition
        
        // emit a row if this is not the beginning of the first
        // partition
        if (!initialState)
        {
          info.out().setString(0, lastPartVal);
          info.out().setString(1, concatResult);
          emitRow(info);
        }
        else
          initialState = false;
        
        // prepare for the next partition
        concatResult = concatToken;
        lastPartVal = currPartVal;
      }
    }
    
    // emit final row, if needed
    if (!initialState)
    {
      info.out().setString(0, lastPartVal);
      info.out().setString(1, concatResult);
      emitRow(info);
    }

  }

}
