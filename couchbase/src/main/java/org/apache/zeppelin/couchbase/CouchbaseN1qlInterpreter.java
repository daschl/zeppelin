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
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * This {@link Interpreter} uses Couchbase Server and N1QL.
 *
 * @author Michael Nitschinger
 * @author Laurent Doguin
 */
public class CouchbaseN1qlInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(CouchbaseN1qlInterpreter.class);

  private volatile Cluster cluster;
  private volatile Bucket bucket;

  public CouchbaseN1qlInterpreter(final Properties property) {
    super(property);
  }

  @Override
  public void open() {
    cluster = CouchbaseCluster.create();
    bucket = cluster.openBucket();
  }

  @Override
  public void close() {
    if (cluster != null) {
      cluster.disconnect();
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    logger.info("Running N1QL command '" + cmd + "'");

    try {
      N1qlQuery n1qlQuery = N1qlQuery.simple(cmd);
      N1qlQueryResult result = bucket.query(n1qlQuery);
      if (!result.finalSuccess()) {
        final StringBuilder buffer = new StringBuilder();
        Iterator<JsonObject> iter = result.errors().iterator();
        while (iter.hasNext()) {
          JsonObject jo = iter.next();
          buffer.append(jo.toString());
        }
        return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT,
          buffer.toString());
      }
      Iterator<N1qlQueryRow> iter = result.rows();
      final List<Map<String, Object>> flattenDocs = new LinkedList<>();
      final Set<String> keys = new TreeSet<>();

      //First : get all the keys in order to build an ordered list of the values for each hit
      //
      while (iter.hasNext()) {
        N1qlQueryRow row = iter.next();
        Map<String, Object> flattenMap = JsonFlattener.flattenAsMap(row.value().toString());
        flattenDocs.add(flattenMap);
        keys.addAll(flattenMap.keySet());
      }

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

      // Finally : build the result by using the key set
      //
      for (final Map<String, Object> hit : flattenDocs) {
        for (final String key : keys) {
          final Object val = hit.get(key);
          if (val != null) {
            buffer.append(val);
          }
          buffer.append('\t');
        }
        buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");
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
