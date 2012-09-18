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

import org.bedework.sysevents.NotificationException;
import org.bedework.sysevents.events.SysEvent;
import org.bedework.sysevents.events.SysEventBase.SysCode;
import org.bedework.sysevents.listeners.JmsSysEventListener;

/** Something that handles system messages. We are looking for messages that
 * might imply a change to the alarms
 *
 * @author Mike Douglass
 *
 */
public class SystemMesssageHandler extends JmsSysEventListener implements Runnable {
  private AlarmHandler handler;

  private String processorQueueName;

  boolean debug;

  /**
   * @param handler
   * @param processorQueueName
   */
  public SystemMesssageHandler(final AlarmHandler handler,
                               final String processorQueueName) {
    this.handler = handler;
    this.processorQueueName = processorQueueName;

    debug = getLogger().isDebugEnabled();
  }

  public void run() {
    /* Start one thread to ping the handler every time something happens */
    try {
      open(processorQueueName);

      process(false);
    } catch (Throwable t) {
      error("AlarmProcessor terminating with exception:");
      error(t);
    }
  }

  @Override
  public void action(final SysEvent ev) throws NotificationException {
    try {
      if (debug) {
        trace("SystemMesssageHandler entry: " + ev);
      }

      if ((ev.getSysCode() != SysCode.ENTITY_ADDED) &&
          (ev.getSysCode() != SysCode.ENTITY_TOMBSTONED) &&
          (ev.getSysCode() != SysCode.ENTITY_DELETED) &&
          (ev.getSysCode() != SysCode.ENTITY_UPDATED)) {
        return;
      }

      if (debug) {
        trace("Ping the alarm handler");
      }

      handler.refresh();
    } catch (Throwable t) {
      error(t);
    }
  }
}
