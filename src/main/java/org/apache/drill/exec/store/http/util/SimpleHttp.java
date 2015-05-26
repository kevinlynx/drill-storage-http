/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.http.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class SimpleHttp {
  public String get(String urlStr) {
    String res = "";
    try {
      URL url = new URL(urlStr);
      URLConnection conn = url.openConnection();
      conn.setDoOutput(true);

      BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
      String line;
      while ((line = rd.readLine()) != null) {
        res += line;
      }
      rd.close();
    } catch (Exception e) {
    }
    return res;
  }

  public static void main(String[] args) {
    SimpleHttp http = new SimpleHttp();
    System.out.println(http.get("http://106.186.122.56:8000/e/api:search?q=avi"));
  }

}
