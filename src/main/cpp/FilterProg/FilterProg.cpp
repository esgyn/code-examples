// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
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

#include "sqludr.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

// FilterProg TMUDF
//
// A "filter" in Linux lingo is a simple program that reads data from
// stdin and writes results to stdout.
//
// FilterProg is a TMUDF that creates a new process and pipes the table-valued
// input (if any) to it as delimited rows. It also reads the output of
// the created process as delimited rows and turns it into output rows.
// 
// Sample invocation of the FilterProg TMUDF:
//
// SELECT *
// FROM UDF(filterprog(
//        -- input table with partition by/order by 
//        TABLE(SELECT a,b,c FROM t PARTITION BY a ORDER BY b),
//        'myprog.py',         -- executable to spawn,
//                             -- path name relative to
//                             -- $MY_SQROOT/udr/public/external_libs
//        'arg1 arg2 "arg 3"', -- command line arguments for new process
//        'IIC20N18.2'))       -- description of the output columns the
//                             -- filter program generates as a list of
//                             -- codes:
//                             --     Code   Column type
//                                    -----  --------------------------------------------
//                                    Cnnnn  Character field with nnnn characters
//                                    D      Date field (2015-12-21)
//                                    F      Floating point field (3.14E0 or 3.14)
//                                    I      Integer field
//                                    L      Long field
//                                    Npp.ss Numeric field with precision pp and scale ss
//                                    Snn    Timestamp field (yyyy-mm-dd hh:mm:ss.ffffff)
//                                           with fraction precision nn (0-6)
//                                    T      Time field (hh:mm:ss)
//                                    $n     Same type as input column n (1-based)
//                                    $*     Types of all input columns
//
//                             -- The example 'IIC20N18.2' above would be
//                             -- (integer, integer, char(20 bytes) character set utf8, numeric(18,2))

// Commands to compile this UDF:

/*
   # source in sqenv.sh to set MY_SQROOT and SQ_MBTYPE
   g++ -g -I$MY_SQROOT/export/include/sql -fPIC -fshort-wchar -c -o FilterProg.o FilterProg.cpp
   g++ -shared -rdynamic -o libfilterprog.so -lc -L$MY_SQROOT/export/lib${SQ_MBTYPE} -ltdm_sqlcli FilterProg.o
 */

// SQL commands to create this UDF:

/*
   drop function filterprog;
   drop library filterproglib;
   create library filterproglib file
    '.../libfilterprog.so';

   create table_mapping function filterprog()
      external name 'TRAF_CPP_FILTERPROG' -- name of factory method
      language cpp
      library filterproglib;

 */


// forward declaration
class FilterProgContext;

// factory method

using namespace tmudr;

// handling of some errors that could cause the launched process to
// fail, keep these two lists in sync

enum ChildExitCodes {
  FIRST_CHILD_EXIT_CODE = 111,
  CLOSE_PIPE_STDIN,
  CLOSE_PIPE_STDOUT,
  DUP_PIPE_STDIN,
  DUP_PIPE_STDOUT,
  CHDIR_CALL,
  PARSE_EXEC_ARGS,
  EXECVE_CALL,
  LAST_CHILD_EXIT_CODE
};

const char *ChildExitErrMessages[] = {
  "unused, first error",
  "Error closing the unused stdin pipe",
  "Error closing the unused stdout pipe",
  "Error redirecting stdin",
  "Error redirecting stdout",
  "Error changing directory to sandbox dir",
  "Error parsing command line arguments for executable",
  "Error during execve() call",
  "unused, last error"
};

// derive a class from UDR

class FilterProg : public UDR
{
public:

  // determine output columns dynamically at compile time
  void describeParamsAndColumns(UDRInvocationInfo &info);

  // override the runtime method
  void processData(UDRInvocationInfo &info,
                   UDRPlanInfo &plan);

  // check for input data for the process
  int processInput(UDRInvocationInfo &info,
                   FilterProgContext &ctx);

  // check for result data coming back from the process
  int processOutput(UDRInvocationInfo &info,
                    FilterProgContext &ctx);

  // check for end (intended or unintended) of child process
  int checkChild(FilterProgContext &ctx);

  // similar to atoi, but stops at non-numerics
  int decodeNumber(const char *s, int &pos);

};

extern "C" UDR * TRAF_CPP_FILTERPROG()
{
  return new FilterProg();
}

// context for one invocation of a FilterProg UDF
struct FilterProgContext
{
  // file descriptors for the pipes to and from the filter
  int inputFDToFilter_;
  int outputFDFromFilter_;

  // I/O buffers
  int readBufLen_;
  char *readBufForOutput_;

  // delimiter characters
  char fieldDelimiter_;
  char recordDelimiter_;
  bool useQuotes_;
  char quote_;

  // error escape sequence in filter output
  const char *filterError_;
  int filterErrorLen_; // length of above string
  // startupFilterError_ must have filterError as a prefix
  const char *startupFilterError_;
  int startupFilterErrorLen_;

  // carry-over data from one call of processInput/Output to the next
  std::string inputRemainderStr_;
  ssize_t inputBytesAlreadyWritten_;
  std::string outputRemainderStr_;

  // state
  bool moreInputRows_;  // is there a potential for more input rows
  bool moreOutputRows_; // has the filter sent EOF (or died)
  bool filterExited_;   // filter program has ended already
  long inputRowNum_;    // input line number for error messages
  long outputLineNum_;  // output line number for error messages
  pid_t filterPid_;     // pid of spawned child

  // constructor/destructor just manages allocated resources
  FilterProgContext() { readBufLen_ = 0; readBufForOutput_ = NULL; }
  ~FilterProgContext() { delete readBufForOutput_; }
};

void FilterProg::describeParamsAndColumns(UDRInvocationInfo &info)
{
  std::string outputCols;

  // validate input parameters
  if (info.par().getNumColumns() != 3)
    throw UDRException(
         38010,
         "Expecting three scalar input parameters, executable, command line arguments and types of output columns");

  if (info.par().isAvailable(2))
    outputCols = info.par().getString(2);
  else
    throw UDRException(
         38020,
         "The third parameter must be a compile time constant with the data types of the output columns");

  // function type, this mainly determines how the TMUDF can be parallelized
  if (info.getNumTableInputs() > 0)
    // set the function type either to a mapper or to a reducer, depending on the query
    if (info.in().getQueryPartitioning().getNumEntries() > 0)
      // since we work locally in a partition, set the function type
      // of this TMUDF to REDUCER
      info.setFuncType(UDRInvocationInfo::REDUCER);
    else
      info.setFuncType(UDRInvocationInfo::MAPPER);
  else
    ; // leave function type as is, do not parallelize for now

  // produce output columns
  const char *oc = outputCols.c_str();
  int numc = strlen(oc);
  int pos = 0;
  int numOutCols = 0;
  char buf[20];

  while (pos < numc)
    {
      TypeInfo::SQLTypeCode typeCode;
      int typeLength = 0;
      int scale = 0;
      int precision = 0;
      bool columnAlreadyAdded = false;

      switch (oc[pos])
        {
        case 'C':
          // char / varchar
          typeCode = TypeInfo::VARCHAR;
          typeLength = decodeNumber(oc, ++pos);
          if (typeLength <= 0)
            throw UDRException(38030,
                               "Invalid character length at position %d in type arguments %s",
                               pos, oc);
          break;

        case 'D':
          // date
          typeCode = TypeInfo::DATE;
          pos++;
          break;

        case 'F':
          // float / double
          typeCode = TypeInfo::DOUBLE_PRECISION;
          pos++;
          break;

        case 'I':
          // int
          typeCode = TypeInfo::INT;
          pos++;
          break;

        case 'L':
          // largeint
          typeCode = TypeInfo::LARGEINT;
          pos++;
          break;

        case 'N':
          // numeric
          typeCode = TypeInfo::NUMERIC;
          precision = decodeNumber(oc, ++pos);
          if (precision <= 0)
            throw UDRException(38040,
                               "Invalid number of numeric digits at position %d in type arguments %s",
                               pos, oc);
          if (oc[pos] == '.')
            scale = decodeNumber(oc, ++pos);
          break;

        case 'S':
          // timestamp
          typeCode = TypeInfo::TIMESTAMP;
          scale = decodeNumber(oc, ++pos);
          break;

        case 'T':
          // time
          typeCode = TypeInfo::TIME;
          pos++;
          break;

        case '$':
          // take type from input column
          {
            int startCol;
            int numCols = 1;

            if (oc[++pos] == '*')
              {
                startCol = 0;
                numCols = info.in().getNumColumns();
                pos++;
              }
            else
              startCol = decodeNumber(oc, pos)-1; // startCol is 0-based

            // add output columns with the same name and type as the
            // corresponding input columns (note these are NOT
            // declared as passthru columns)
            for (int c=startCol; c<startCol+numCols; c++)
              info.out().addColumn(info.in().getColumn(c));

            columnAlreadyAdded = true;
          }
          break;

        default:
          throw UDRException(38060,
                             "unrecognized output column type at position %d in argument %s",
                             pos, oc);
        }

      if (!columnAlreadyAdded)
        {
          // make up an output column name
          snprintf(buf, sizeof(buf), "C%d", ++numOutCols);

          info.out().addColumn(ColumnInfo( 
                                    buf,
                                    TypeInfo(typeCode,
                                             typeLength,
                                             true, // nullable
                                             scale,
                                             TypeInfo::CHARSET_UTF8,
                                             TypeInfo::UNDEFINED_INTERVAL_CODE,
                                             precision)));
        }
    }

  // add formal parameters with types that match the actual ones
  for (int p=0; p<info.par().getNumColumns(); p++)
    {
      char parName[20];

      snprintf(parName, sizeof(parName), "PAR_%d", p);
      info.addFormalParameter(ColumnInfo(parName,
                                         info.par().getColumn(p).getType()));
    } 
}

void FilterProg::processData(UDRInvocationInfo &info,
                             UDRPlanInfo &plan)
{
  std::string sandboxDir;
  std::string executable;
  int pipeForStdin[2];
  int pipeForStdout[2];
  FilterProgContext ctx;

  // create a pair of pipes to talk to the new process
  pipe(pipeForStdin);
  pipe(pipeForStdout);

  // We don't want this UDF to be able to call any executable on the
  // system, so we sandbox it into a directory. When placing executables
  // into that directory ($MY_SQROOT/udr/public/external_libs),
  // be careful to consider the security of your system.

  std::string exeParameter =  info.par().getString(0);

  if (exeParameter.c_str()[0] == '/')
    throw UDRException(
         38100,
         "No absolute executable names allowed, names must be relative to $MY_SQROOT/udr/public/external_libs");

  if (exeParameter.find("..") != std::string::npos)
    throw UDRException(
         38102,
         "Parent directory .. is not allowed in executable name %s",
         exeParameter.c_str());

  struct stat exeStat;
  sandboxDir = getenv("TRAF_HOME");

  if (sandboxDir.empty())
    throw UDRException(
         38110,
         "Unable to read $MY_SQROOT environment variable");

  if (sandboxDir.at(sandboxDir.length()-1) != '/')
    sandboxDir.append("/");
  sandboxDir.append("udr/public/external_libs/");
  executable = sandboxDir;
  executable.append(exeParameter);

  int retcode = lstat(executable.c_str(), &exeStat);

  if (retcode != 0)
    throw UDRException(
         38120,
         "Unable to stat executable file %s, error: %d",
         executable.c_str(),
         (int) errno);

  if (!S_ISREG(exeStat.st_mode))
    throw UDRException(
         38130,
         "File %s is not a regular file",
         executable.c_str());

  // note we haven't checked yet whether we have execute permissions
  // on this file

  // initialize the context
  ctx.inputFDToFilter_ = pipeForStdin[1];
  ctx.outputFDFromFilter_ = pipeForStdout[0];
  ctx.readBufLen_ = 200001;
  ctx.readBufForOutput_ = new char[ctx.readBufLen_];
  ctx.fieldDelimiter_ = '|';
  ctx.recordDelimiter_ = '\n';
  ctx.useQuotes_ = true;
  ctx.quote_ = '"';
  ctx.filterError_ = "#_#_#_#_#_#_#_#_#_#_: ";
  ctx.filterErrorLen_ = strlen(ctx.filterError_);
  ctx.startupFilterError_ = "#_#_#_#_#_#_#_#_#_#_##STARTUP##: ";
  ctx.startupFilterErrorLen_ = strlen(ctx.startupFilterError_);
  ctx.inputBytesAlreadyWritten_ = 0;
  ctx.moreInputRows_ = (info.getNumTableInputs() > 0);
  ctx.moreOutputRows_ = true;
  ctx.filterExited_ = false;
  ctx.inputRowNum_ = 1;
  ctx.outputLineNum_ = 1;
  ctx.filterPid_ = 0;

  // fork a new process

  ctx.filterPid_ = fork();

  if (ctx.filterPid_ == 0)
    {
      // ---------------------------------------------
      // code in the child process, the filter process
      // ---------------------------------------------

      // close the unused ends of the pipes
      if (close(pipeForStdin[1]))
        exit(CLOSE_PIPE_STDIN);
      if (close(pipeForStdout[0]))
        exit(CLOSE_PIPE_STDOUT);

      // hook up the pipes to stdin and stdout
      if (dup2(pipeForStdin[0], STDIN_FILENO) < 0)
        exit(DUP_PIPE_STDIN);
      if (dup2(pipeForStdout[1], STDOUT_FILENO) < 0)
        exit(DUP_PIPE_STDOUT);

      // cd to the sandbox dir
      if (chdir(sandboxDir.c_str()))
        {
          printf("%s Error %d changing current directory to %s\n",
                 ctx.startupFilterError_,
                 (int) errno,
                 sandboxDir.c_str());
          exit(CHDIR_CALL);
        }

      // add a few environment variables that could be useful for the new executable
      char buf[20];

      snprintf(buf, sizeof(buf), "%d",
               (info.getNumTableInputs() > 0 ? info.in().getNumColumns() : 0));
      setenv("FILTER_UDR_NUM_INPUT_COLUMNS", buf, 1);
      snprintf(buf, sizeof(buf), "%d",
               info.out().getNumColumns());
      setenv("FILTER_UDR_NUM_OUTPUT_COLUMNS", buf, 1);
      snprintf(buf, sizeof(buf), "%c",
               ctx.fieldDelimiter_);
      setenv("FILTER_UDR_FIELD_DELIMITER", buf, 1);
      snprintf(buf, sizeof(buf), "%d",
               info.getNumParallelInstances());
      setenv("FILTER_UDR_NUM_INSTANCES", buf, 1);
      snprintf(buf, sizeof(buf), "%d",
               info.getMyInstanceNum());
      setenv("FILTER_UDR_MY_INSTANCE_NUM", buf, 1);
               
      // parse args into a list of arguments
      std::string argsParameter = info.par().getString(1);
      int argsParLen = argsParameter.size();
      const int maxArgs = 101;
      char *args[maxArgs+1];
      int numArgs = 0;
      const char *a = argsParameter.c_str();
      int pos = 0;

      // executable is first arg
      args[numArgs++] = const_cast<char *>(executable.c_str());

      // loop over characters of the argument string and try to
      // tokenize it into pieces separated by blanks or tabs
      while (pos<argsParLen)
        {
          // skip over initial unquoted whitespace
          while (isspace(a[pos]))
            pos++;

          // we reached some non-white space, this is an argument
          int argStart = pos;
          bool inSingleQuote = false;
          bool inDoubleQuote = false;

          // traverse the non-blank or quoted text
          while (pos<argsParLen &&
                 (!isspace(a[pos]) || inSingleQuote || inDoubleQuote))
            {
              if (a[pos] == '\'')
                inSingleQuote = !inSingleQuote;
              else if (a[pos] == '"')
                inDoubleQuote = !inDoubleQuote;
              pos++;
            }

          int newArgLen = pos-argStart;
          char *newArg = new char[newArgLen+1];
          strncpy(newArg, &a[argStart], newArgLen);
          newArg[newArgLen] = '\0';

          if (numArgs >= maxArgs)
            {
              printf("%s Too many arguments (%d) for executable, limit is %d\n",
                     ctx.startupFilterError_,
                     numArgs+1,
                     maxArgs);
              exit(PARSE_EXEC_ARGS);
            }
          args[numArgs++] = newArg;
        }
      
      args[numArgs] = NULL;

      execve(executable.c_str(),
             args,
             environ);

      // if we reach here we failed to launch the executable, print an
      // error message and exit with a special code, both should
      // trigger error messages in the parent process (the better one
      // coming from the first method)
      printf("%s Error %d in execve for executable $s\n",
             ctx.startupFilterError_,
             (int) errno,
             executable.c_str());
      exit(EXECVE_CALL);
    }
  
  // -------------------------------
  // code in the parent (UDR server)
  // -------------------------------

  // close the unused ends of the pipes
  if (close(pipeForStdin[0]))
    throw UDRException(38140, "Error closing stdin pipe");
  if (close(pipeForStdout[1]))
    throw UDRException(38150, "Error closing stdout pipe");

  // set the stdin pipe to the child and the stdout pipe from the
  // child process to allow non-blocking reads, but only if we also
  // need to send input data to the process
  if (ctx.moreInputRows_)
    {
      if (fcntl(ctx.inputFDToFilter_, F_SETFL, O_NONBLOCK))
        throw UDRException(38160, "Error setting stdin pipe to non-blocking");
      if (fcntl(ctx.outputFDFromFilter_, F_SETFL, O_NONBLOCK))
        throw UDRException(38170, "Error setting stdout pipe to non-blocking");
    }

  // --------------------------------------------------------------------
  // main loop processes input and output of the pipe process interleaved
  // --------------------------------------------------------------------
  while (processInput(info, ctx) + processOutput(info, ctx) + checkChild(ctx) > 0)
    ;

}

// this method handles reading new input rows and sending them
// (non-blocking) to the filter program

int FilterProg::processInput(UDRInvocationInfo &info, FilterProgContext &ctx)
{
  // simplest case, we are not expecting any (more) input rows
  if (!ctx.moreInputRows_)
    return 0;

  // returns true if we should keep calling this method, false if we are done processing input
  int result = 1;

  std::string freshInputStr;
  int remainderLen = ctx.inputRemainderStr_.size();

  if (remainderLen == 0)
    {
      // ----------------------------------------------------------
      // we need a new input row
      // ----------------------------------------------------------
      result = (getNextRow(info) ? 1 : 0);

      if (result)
        {
          // we got an input row to send to our filter program,
          // construct a delimited row and write it to the pipe
          info.in().getDelimitedRow(freshInputStr,
                                    ctx.fieldDelimiter_,
                                    ctx.useQuotes_,
                                    ctx.quote_);
          // append a record delimiter
          freshInputStr.append(1, ctx.recordDelimiter_);
        }
      else
        {
          // we've reached EOD for the input rows, close the pipe to the
          // process
          if (close(ctx.inputFDToFilter_))
            throw UDRException(38300,
                               "Error %d closing pipe to filter after EOD",
                               (int) errno);
          ctx.moreInputRows_ = false;
        }
    }

  if (result)
    {
      // Now we have either a leftover from a previous call to write
      // or a fresh delimited row that we just read. Send all or part
      // of it to the filter process.

      const char *writeBuf = NULL;
      size_t bytesToWrite = 0;

      if (remainderLen > 0)
        {
          // write the remainder of the remainder string, the part that
          // has not yet been written
          writeBuf = ctx.inputRemainderStr_.data() + ctx.inputBytesAlreadyWritten_;
          bytesToWrite = remainderLen - ctx.inputBytesAlreadyWritten_;
        }
      else
        {
          // write the fresh string
          writeBuf = freshInputStr.data();
          bytesToWrite = freshInputStr.size();
        }

      // try to write some data to the pipe that is connected with the
      // filter's stdin
      ssize_t bytesWritten = write(ctx.inputFDToFilter_,
                                   writeBuf,
                                   bytesToWrite);

      if (bytesWritten == bytesToWrite)
        {
          // we successfully sent one input line to the filter process
          ctx.inputRowNum_++;
          if (remainderLen)
            {
              // clear the remainder, we are done
              ctx.inputRemainderStr_.clear();
              ctx.inputBytesAlreadyWritten_ = 0;
            }
        }
      else if (bytesWritten >= 0)
        {
          // success, but not all data was written
          if (remainderLen)
            {
              // update the length of the remainder we have
              // already written (it's going to take multiple
              // tries)
              ctx.inputBytesAlreadyWritten_ += bytesWritten;
            }
          else
            {
              // copy whatever is left of the fresh string into the
              // remainder string
              ctx.inputRemainderStr_.assign(freshInputStr.data() + bytesWritten,
                                            bytesToWrite - bytesWritten);
              ctx.inputBytesAlreadyWritten_ = 0;
            }
        }
      else // bytesWritten < 0
        {
          if (errno != EAGAIN)
            throw UDRException(
                 38310,
                 "Error %d while sending input data to the pipe process at input row number %ld",
                 (int) errno,
                 ctx.inputRowNum_);
        }
    } // result

  return result;
}

// this method handles receiving result rows (non-blocking) from the
// filter program and sending them as result rows of the UDF

int FilterProg::processOutput(UDRInvocationInfo &info, FilterProgContext &ctx)
{
  char *delimOutRow = ctx.readBufForOutput_; // pointer to next delimited row
  char *endOfBuffer = NULL;                  // pointer to end of read data
  int bufferLen = ctx.readBufLen_ - 1;       // reserve one for trailing '\0'
  int remainderLen = ctx.outputRemainderStr_.size();  // remainder length
  char *bufferForRead = &ctx.readBufForOutput_[remainderLen];

  // try to read more data, leave room in the buffer to insert the
  // remainderStr in the front
  ssize_t bytesRead = read(ctx.outputFDFromFilter_,
                           bufferForRead,
                           bufferLen - remainderLen);

  if (bytesRead < 0)
    {
      if (errno == EAGAIN)
        // need to wait for more output
        return 1;

      throw UDRException(38320,
                         "Error %d during read from process output",
                         (int) errno);
    }

  if (bytesRead == 0 && remainderLen == 0)
    // this indicates EOF, we are done
    return 0;

  // if there is a remainder (partial record) from the last call,
  // prepend it to the buffer into the space we left for it
  if (remainderLen > 0)
    {
      // should leave at least 2 bytes, one for read, one for nul terminator
      if (remainderLen >= ctx.readBufLen_-2)
        // should have caught this with exception 38580 below
        throw UDRException(38330, "Internal error");
      memcpy(ctx.readBufForOutput_, ctx.outputRemainderStr_.data(), remainderLen);
    }

  // we now have remainderLen + bytesRead bytes of data to process
  endOfBuffer = ctx.readBufForOutput_ + remainderLen + bytesRead;

  while (delimOutRow)
    {
      bool foundEOL = false;
      bool inQuotes = false;
      char *eol = delimOutRow; // pointer to the end of a delimited row or part of it

      // This is for sending simple error messages back from the filter program:
      // Lines with a certain prefix are treated as exceptions.
      if (endOfBuffer-delimOutRow > ctx.filterErrorLen_ &&
          memcmp(ctx.filterError_, delimOutRow, ctx.filterErrorLen_) == 0)
        {
          // add a nul terminator so we can print the message
          *endOfBuffer = '\0';

          if (endOfBuffer-delimOutRow > ctx.startupFilterErrorLen_ &&
              memcmp(ctx.startupFilterError_,
                     delimOutRow,
                     ctx.startupFilterErrorLen_) == 0)
            throw UDRException(
                 38340,
                 "Error during startup of filter program: %s",
                 delimOutRow + ctx.startupFilterErrorLen_);
          else
            throw UDRException(
                 38350,
                 "Filter program emitted an error message: %s",
                 delimOutRow + ctx.filterErrorLen_);
        }

      // find an unquoted record delimiter, also count lines on the way
      while (eol != endOfBuffer && !foundEOL)
        {
          if (*eol == ctx.recordDelimiter_)
            {
              ctx.outputLineNum_++;
              foundEOL = !inQuotes;
            }
          else if (*eol == ctx.quote_)
            inQuotes = !inQuotes;

          if (!foundEOL)
            eol++;
        }

      if (foundEOL || bytesRead == 0)
        {
          // we found an unquoted record delimiter or we are the end
          // of the buffer and we reached EOF and did not find a
          // record delimiter

          // NUL terminate the row (overwrites record delimiter or buffer
          // space after the read data or the extra byte we left at
          // the end of the buffer)
          *eol = '\0';

          // now that we got the delimited row, convert it to the right
          // data types and emit it

          info.out().setFromDelimitedRow(delimOutRow, ctx.fieldDelimiter_, true);

          // ----------------------------------------------------------
          // emit output row
          // ----------------------------------------------------------
          emitRow(info);

          if (++eol < endOfBuffer)
            // there is at least part of another row in the buffer we read
            delimOutRow = eol;
          else
            // we consumed the entire buffer
            delimOutRow = NULL;
        }
      else
        {
          // We did not process all the read data. This could be due to
          // several reasons:
          // - read() call returned a partial record
          // - read() returned last record without a record terminator
          // - user error, e.g. missing record terminators or unbalanced quotes
          int newRemainderLen = endOfBuffer - delimOutRow;
          int lengthLimit = ctx.readBufLen_-10;

          if (newRemainderLen > lengthLimit)
            // don't let this grow too long, we need to be able to store this in
            // ctx.readBufForOutput_
            throw UDRException(38360,
                               "Encountered a record that is too long (%d bytes) in output line %ld, maybe due to missing record delimiter or unbalanced quotes, current limit is %d",
                               newRemainderLen,
                               ctx.outputLineNum_,
                               lengthLimit);

          ctx.outputRemainderStr_.append(delimOutRow, newRemainderLen);
          delimOutRow = NULL;
        }
    } // while (delimOutRow)

  // maybe processed some rows, expecting more rows
  return 1;
}

int FilterProg::checkChild(FilterProgContext &ctx)
{
  int filterExitStatus = 0;
  int waitOptions = 0;

  if (ctx.filterExited_ || ctx.filterPid_ == 0)
    return 0;

  // blocking wait at the end, just poll otherwise
  if (ctx.moreOutputRows_)
    waitOptions |= WNOHANG;

  // exit handling, wait for the spawned filter process to terminate
  pid_t waitResult = waitpid(ctx.filterPid_, &filterExitStatus, waitOptions);

  if (waitResult >= 0 && waitResult != ctx.filterPid_)
    // nothing happened to our filter process
    return 1;

  // check for error returns
  if (WIFEXITED(filterExitStatus))
    {
      int exitCode = WEXITSTATUS(filterExitStatus);
      if (exitCode != 0)
        {
          if (exitCode > FIRST_CHILD_EXIT_CODE &&
              exitCode < LAST_CHILD_EXIT_CODE)
            // this is the range of code we produce in this file,
            // but the UDF program might also use the same codes
            throw UDRException(38180,
                               "Child process exited with status %d, likely cause is %s",
                               exitCode,
                               ChildExitErrMessages[exitCode-FIRST_CHILD_EXIT_CODE]);
          else
            throw UDRException(38190,
                               "Child process exited with status %d",
                               exitCode);
        }
      // -------------------------------------------------------
      // if we reach here then the child process exited normally
      // -------------------------------------------------------
      ctx.filterExited_ = true;
      ctx.filterPid_ = 0;
      return 0;
    }
  else if (WIFSIGNALED(filterExitStatus))
    {
      if (WCOREDUMP(filterExitStatus))
        throw UDRException(38200,
                           "Child process terminated with signal %d and produced a core dump",
                           (int) WTERMSIG(filterExitStatus));
      else
        throw UDRException(38210,
                           "Child process terminated with signal %d",
                           (int) WTERMSIG(filterExitStatus));
    }
  else
    throw UDRException(38220,
                       "Child process stopped during tracing or received SIGCONT");

  // never reaches here
}

int FilterProg::decodeNumber(const char *s, int &pos)
{
  int result = 0;

  while (s[pos] >= '0' && s[pos] <= '9')
    result = 10*result + s[pos++] - '0';

  return result;
}
