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

import org.bedework.calfacade.BwAlarm;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calsvci.CalSvcFactoryDefault;
import org.bedework.calsvci.CalSvcI;
import org.bedework.calsvci.CalSvcIPars;

import edu.rpi.sss.util.Util;

import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/** Build a small queue of pending events. The queue will be updated as system
 * messages indicate a possible change to th ealarms.
 *
 * @author Mike Douglass
 */
public class AlarmHandler implements Runnable {
  protected boolean debug;

  private transient Logger log;

  private CalSvcI svci;

  private String principal;

  /** */
  public static final String stateStopped = "Stopped";

  /** */
  public static final String stateRunning = "Running";

  /** */
  public static final String stateSleeping = "Sleeping";

  private String state = stateStopped;

  private Queue<BwAlarm> alarms = new LinkedList<BwAlarm>();

  private boolean running = false;

  private long emptyWaitTime = 60 * 1000;

  /**
   * @param principal
   */
  public AlarmHandler(final String principal) {
    this.principal = principal;
  }

  public void run() {
    try {
      running = true;
      state = stateRunning;

      process();
    } catch (Throwable t) {
      error("AlarmHandler terminating with exception:");
      error(t);
    }
  }

  /** Called to indicate queue needs a refresh
   *
   * @throws CalFacadeException
   */
  public void refresh() throws CalFacadeException {
    getSvci();

    synchronized (svci) {
      try {
        openSvci();

        long triggerTime = System.currentTimeMillis() + (1000 * 60);

        Collection<BwAlarm> as = svci.getAdminHandler().getUnexpiredAlarms(triggerTime);

        alarms.clear();

        if (!Util.isEmpty(as)) {
          for (BwAlarm a: as) {
            BwAlarm ca = (BwAlarm)a.clone();
            ca.setId(a.getId());

            alarms.add(ca);
          }
        }
      } finally {
        closeSvci();
      }
    }
  }

  private void process() {
    while (running) {
      try {
        boolean didSomething = false;
        state = stateRunning;

        getSvci();

        synchronized (svci) {
          if (!alarms.isEmpty()) {
            BwAlarm alarm = alarms.peek();

            processAlarm(alarm);

            alarms.remove();
          }

        }

        if (!didSomething) {
          state = stateSleeping;
          Thread.sleep(emptyWaitTime);
        }
      } catch (InterruptedException ie) {
        // Somebody pinged us
      } catch (Throwable t) {

      }
    }
  }

  /**
   *
   */
  public void stop() {
    running = false;
  }

  protected void trace(final String msg) {
    getLogger().debug("trace: " + msg);
  }

  protected void warn(final String msg) {
    getLogger().warn(msg);
  }

  protected void error(final Throwable t) {
    getLogger().error(this, t);
  }

  protected void error(final String msg) {
    getLogger().error(msg);
  }

  /* ====================================================================
   *                   Private methods
   * ==================================================================== */

  private void processAlarm(final BwAlarm alarm) throws CalFacadeException {
    boolean hadError = false;

    getSvci();

    synchronized (svci) {
      try {
        if (debug) {
          trace("Alarm handler: " + alarm);
        }

        Collection<BwEvent> evs = svci.getAdminHandler().getEventsByAlarm(alarm);

        if (debug) {
          trace("Events found: " + evs.size());
        }

      } catch (Throwable t) {
        error(t);
        hadError = true;
      } finally {
        try {
          closeSvci();
        } catch (Throwable t) {}
      }

      if (hadError) {
        svci = null;
      }
    }
  }

  /** Get an svci object
   *
   * @throws CalFacadeException
   */
  private synchronized void getSvci() throws CalFacadeException {
    if (svci != null) {
      return;
    }

    CalSvcIPars runAsPars = CalSvcIPars.getServicePars(principal,
                                                  false,   // publicAdmin,
                                                  "/principals/users/root".equals(principal));  // allow SuperUser

    svci = new CalSvcFactoryDefault().getSvc(runAsPars);
  }

  private void openSvci() throws CalFacadeException {
    svci.open();
    svci.beginTransaction();
  }

  private void closeSvci() throws CalFacadeException {
    if ((svci == null) || !svci.isOpen()) {
      return;
    }

    svci.endTransaction();
    svci.close();
  }

  /* Get a logger for messages
   */
  private Logger getLogger() {
    if (log == null) {
      log = Logger.getLogger(this.getClass());
    }

    return log;
  }
}
