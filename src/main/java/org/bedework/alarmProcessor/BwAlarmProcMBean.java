/* ********************************************************************
    Licensed to Jasig under one or more contributor license
    agreements. See the NOTICE file distributed with this work
    for additional information regarding copyright ownership.
    Jasig licenses this file to you under the Apache License,
    Version 2.0 (the "License"); you may not use this file
    except in compliance with the License. You may obtain a
    copy of the License at:
        
    http://www.apache.org/licenses/LICENSE-2.0
        
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied. See the License for the
    specific language governing permissions and limitations
    under the License.
*/
package org.bedework.alarmProcessor;

/**
 * @author douglm
 *
 */
public interface BwAlarmProcMBean {
  /** Principal we run under
   *
   * @param val
   */
  public void setPrincipal(String val);

  /**
   * @return String principal we use
   */
  public String getPrincipal();

  /**
   * @param val
   */
  public void setProcessorQueueName(final String val);

  /**
   * @return String processorQueueName we use
   */
  public String getProcessorQueueName();

  /**
   * @return long number of alarms processed
   */
  public long getProcessedCount();

  /**
   * @return String next alarm time
   */
  public String getNextAlarmTime();

  /** Name apparently must be the same as the name attribute in the
   * jboss service definition
   *
   * @return Name
   */
  public String getName();

  /** Lifecycle
   *
   */
  public void start();

  /** Lifecycle
   *
   */
  public void stop();

  /** Lifecycle
   *
   * @return true if started
   */
  public boolean isStarted();

}
