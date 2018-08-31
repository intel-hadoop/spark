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

package org.apache.spark.util

import scala.collection.mutable
import scala.xml.XML

import org.apache.spark.internal.Logging

/**
 * A xml parser used for persistent memory config file parse.
 */
private[spark] object XmlUtils extends Logging {
  private val PERSISTENT_MEMORY_CONFIG_FILE = "persistent-memory.xml"
  private val NUMA_NODES_PROPERTY = "numanode"
  private val NUMA_NODE_ID_PROPERTY = "@id"
  private val INITIALIZE_PATH_PROPERTY = "initialPath"
  private var numaToPathMapping = new mutable.HashMap[Int, String]()

  /**
   * Parse the persistent memory xml file.
   * @return The numa id -> initial path mapping.
   */
  def parsePersistentMemoryConfig(): mutable.HashMap[Int, String] = {
    // if already parsed, just return it.
    if (numaToPathMapping.size == 0) {
      val xml = XML.load(
        Utils.getSparkClassLoader.getResourceAsStream(PERSISTENT_MEMORY_CONFIG_FILE)
      )
      for (numaNode <- (xml \\ NUMA_NODES_PROPERTY)) {
        val numaNodeId = (numaNode \ NUMA_NODE_ID_PROPERTY).text.trim.toInt
        val path = (numaNode \ INITIALIZE_PATH_PROPERTY).text
        numaToPathMapping += ((numaNodeId, path))
      }
    }

    numaToPathMapping
  }

  /**
   * The total numa node number.
   */
  def totalNumaNode(): Int = {
    if (numaToPathMapping.size == 0) {
      parsePersistentMemoryConfig()
    }

    numaToPathMapping.size
  }

}
