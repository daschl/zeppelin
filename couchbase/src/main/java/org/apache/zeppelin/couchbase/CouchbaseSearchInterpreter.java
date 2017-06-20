/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.search.SearchConsistency;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This {@link Interpreter} uses Couchbase Server and Full Text Search.
 *
 * @author Michael Nitschinger
 * @author Laurent Doguin
 */
public class CouchbaseSearchInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(CouchbaseSearchInterpreter.class);

  private volatile Cluster cluster;
  private volatile Bucket bucket;

  public CouchbaseSearchInterpreter(final Properties property) {
    super(property);
  }

  @Override
  public void open() {
    cluster = CouchbaseCluster.create();
    bucket = cluster.openBucket("travel-sample");
  }

  @Override
  public void close() {
    if (cluster != null) {
      cluster.disconnect();
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    logger.info("Running FTS command '" + cmd + "'");

    try {
      SearchQuery query = new SearchQuery("idx", SearchQuery.queryString(cmd))
        .searchConsistency(SearchConsistency.NOT_BOUNDED);

      SearchQueryResult result = bucket.query(query);
      if (!result.errors().isEmpty()) {
        final StringBuilder buffer = new StringBuilder();
        Iterator<String> iter = result.errors().iterator();
        while (iter.hasNext()) {
          String jo = iter.next();
          buffer.append(jo);
        }
        return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT,
          buffer.toString());
      }
      final Set<String> keys = new TreeSet<>();
      keys.add("id");
      keys.add("score");


      // Next : build the header of the table
      //
      final StringBuilder buffer = new StringBuilder();
      for (final String key : keys) {
        buffer.append(key).append('\t');
      }

      if (buffer.length() > 0) {
        buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");
      } else {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT,
          "Empty result");
      }

      for (SearchQueryRow row : result.hits()) {
        buffer.append(row.id()).append('\t').append(row.score()).append('\n');
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
        buffer.toString());
    } catch (final CouchbaseException e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
    }
  }


  @Override
  public void cancel(final InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(final InterpreterContext context) {
    return 0;
  }
}
