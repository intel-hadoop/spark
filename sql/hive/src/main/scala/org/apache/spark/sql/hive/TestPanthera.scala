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
package org.apache.spark.sql.hive
import org.apache.spark.SparkContext
object TestPanthera {
  def main(args: Array[String]) {
    val spark = new SparkContext("local", "Panthera", System.getenv("Spark_HOME"), SparkContext.jarOfClass(this.getClass))
    val lhive = (new LocalHiveContext(spark))
    lhive.sql(s"""drop database IF EXISTS hive CASCADE""")
    lhive.sql(s"""create database hive""")
    lhive.sql(s"""use hive""")
    lhive.sql(s"""CREATE TABLE SUPPLIER(S_SUPPKEY INT,S_NAME STRING,S_ADDRESS STRING,S_NATIONKEY INT,S_PHONE STRING,S_ACCTBAL DOUBLE,S_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE""")
    lhive.sql(s"""LOAD DATA LOCAL INPATH '_datapath_/plusd/tpch/supplier.tbl' OVERWRITE INTO TABLE SUPPLIER""")
    lhive.sql(s"""CREATE TABLE PARTSUPP(PS_PARTKEY INT,PS_SUPPKEY INT,PS_AVAILQTY INT,PS_SUPPLYCOST DOUBLE,PS_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE""")
    lhive.sql(s"""LOAD DATA LOCAL INPATH '_datapath_/plusd/tpch/partsupp.tbl' OVERWRITE INTO TABLE PARTSUPP""")
    lhive.sql(s"""CREATE TABLE NATION(N_NATIONKEY INT,N_NAME STRING,N_REGIONKEY INT,N_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE""")
    lhive.sql(s"""LOAD DATA LOCAL INPATH '_datapath_/plusd/tpch/nation.tbl' OVERWRITE INTO TABLE NATION""")
    lhive.sql(s"""select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by value desc""")
  }

}
