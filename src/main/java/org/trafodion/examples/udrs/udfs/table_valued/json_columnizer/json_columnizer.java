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

package org.trafodion.examples.udrs.udfs.table_valued.json_columnizer;

import org.trafodion.sql.udr.*;
import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.StringReader;

class json_columnizer extends UDR {
	/* ===================================================================================
	 *
	 * This is a Table Mapping UDF for Trafodion
	 *
	 * Input table contains a single VARCHAR/STRING column that is a complete JSON record
	 * UDF outputs values corresponding to JSON tags passed in the calling statement
	 * A variable number of tags can be passed in
	 *
	 * Tag names must be fully qualified, eg, 'Master.Employee.Middle Initial' for JSON
	 *  {"MASTER": {"EMPLOYEE": { "Middle Initial": "Q" } } } 
	 *
	 *
	 * To invoke this UDF:
	 * 
	 * 	   select * from udf(json_column(table( select * from <your json table> ),
	 *                                         '<tag>', '<tag>', ...) );
	 *                                         
	 * =================================================================================== */
	public json_column() {}
	
	@Override
	public void describeParamsAndColumns(UDRInvocationInfo info)
	 throws UDRException
	{
		// Compile time
		
		// For each input param, create an output column
		// Params are json tag names - convert these to SQL column names
		// For output, cannot distinguish data types so everything is VARCHAR
		
		String in_tag = "";

		if (info.par().getNumColumns() == 0) {
			throw new UDRException(	38101,
		    						"Expecting at least one fully qualifed JSON tag.");
		}
		
		// input parameters...
		for (int i = 0; i < info.par().getNumColumns(); i++) {
			// make formal param
			info.addFormalParameter(info.par().getColumn(i));
			// use input param to create output param
			in_tag = info.par().getString(i);
			// raw json tag can have spaces and special chars - 
			// remove apostrophe & parens; replace space and slash
			in_tag = in_tag.replaceAll("['()]", "").replaceAll("[ /]", "_").toUpperCase();
			info.out().addVarCharColumn(in_tag, 100, true);
		} // for
		
		// set as Mapper so reading can be parallelized
		info.setFuncType(UDRInvocationInfo.FuncType.MAPPER);
	}
	
	@Override
	public void processData(UDRInvocationInfo info,
	                        UDRPlanInfo plan)
	        throws UDRException
	{
		// Execution time
		
		// Each row of input JSON is delivered
		// Process the set of input specs against each row
		// JsonParser consumes the record for each tag search, so parser must be re-init'd for
		//  each pass
		// Tag may not exist in every record, so null is possible
		
		// We only expect a tag to produce a value or null from the parser.  That is, if the tag
		//  specs only to the struct level, parser will raise exception.  Using the example given
		//  above, a tag of 'Master.Employee' fails.
		
		String jsonString = "";
		boolean found = false;
		
		// each row returned is parsed once for each param
		while (getNextRow(info))
		{
			jsonString = info.in(0).getString(0);     // save the column data
			// for each in parameter
			for ( int i = 0; i < info.par().getNumColumns(); i++ ) {
				found = false;
				JsonParser parser = Json.createParser(new StringReader(jsonString));
				JsonParser.Event event = null;
				// split the json tag into constituent parts
				String[] parts = info.par().getString(i).split("\\.");
				
					// do this until all parts exhausted
					for ( String part : parts) {
						while (true)
						 {
							if ( !parser.hasNext() ) {
								// out of json
								found = false;
								break;
							}
							event = parser.next();
							if (event.equals(JsonParser.Event.KEY_NAME) &&
								parser.getString().equalsIgnoreCase(part) )  {
								found = true;
								break;
							}
						 } // end while 
						if (!found) break;   // early exit for-loop           
					} // end for part
					
				if (found) {
					// next() should be VALUE_STRING
					parser.next();
					info.out().setString(i, parser.getString());
				}
				else
					info.out().setNull(i);
				
			} // for i
			
			// params all done spit out a row
			emitRow(info);
		} // while rows
			
	} // end processData
	
}
