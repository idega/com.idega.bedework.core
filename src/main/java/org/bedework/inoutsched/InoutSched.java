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
package org.bedework.inoutsched;

import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.inoutsched.ScheduleMesssageHandler.ProcessMessageResult;
import org.bedework.sysevents.NotificationException;
import org.bedework.sysevents.events.EntityQueuedEvent;
import org.bedework.sysevents.events.SysEvent;
import org.bedework.sysevents.listeners.JmsSysEventListener;

import edu.rpi.sss.util.Args;

/** Listener class which handles scheduling system events sent via JMS.
 *
 * <p>There are two invocations running, one which handles inbound scheduling
 * messages and one which handles outbound.
 *
 * <p>JMS messages are delivered to the appropriate service so we don't check
 * them here.
 *
 * <p>Inbound messages handle messages that have been delivered to the inbox and
 * which need processing, in effect we act as a virtual client. If autorespond
 * is on we also send out the response.
 *
 * <p>Outbound messages processing handles messages left in the outbox. At the
 * moment we are only handling mail messages. We really should handle all
 * outbound processing - i.e. the sender should just copy the message to their
 * outbox and let the asynch process handle the rest.
 *
 * @author Mike Douglass
 */
public class InoutSched extends JmsSysEventListener implements Runnable {
  private ScheduleMesssageCounts counts;

  private int retryLimit = 10;

  boolean debug;

  /** Constructor to process args
   *
   * @param counts
   * @param retryLimit
   */
  public InoutSched(final ScheduleMesssageCounts counts,
                    final int retryLimit) {
    this.retryLimit = retryLimit;
    this.counts = counts;
  }

  /** Constructor to run
   *
   * @param counts
   * @param retryLimit
   * @param in
   * @throws CalFacadeException
   */
  InoutSched(final ScheduleMesssageCounts counts,
             final int retryLimit,
             final boolean in) throws CalFacadeException {
    this.in = in;
    this.retryLimit = retryLimit;
    this.counts = counts;
  }

  /** Set the number of times we retry a message when we get stale state
   * exceptions.
   *
   * @param val
   */
  public void setRetryLimit(final int val) {
    retryLimit = val;
  }

  /**
   * @return current limit
   */
  public int getRetryLimit() {
    return retryLimit;
  }

  boolean processArgs(final Args args) throws Throwable {
    if (args == null) {
      return true;
    }

    while (args.more()) {
      if (args.ifMatch("")) {
        continue;
      }

      if (args.ifMatch("-appname")) {
        args.next(); // Not used at the moment
      } else if (args.ifMatch("-start")) {
        // Just swallow it
      } else {
        error("Illegal argument: " + args.current());
        usage();
        return false;
      }
    }

    return true;
  }

  void usage() {
    info("Usage:");
    info("       -appname <name>");
    info("");
  }

  private boolean in;

  private ScheduleMesssageHandler handler;

  public void run() {
    try {
      if (in) {
        open(schedulerInQueueName);
        handler = new InScheduler();
      } else {
        open(schedulerOutQueueName);
        handler = new OutScheduler();
      }

      process(false);
    } catch (Throwable t) {
      error("Scheduler(" + in + ") terminating with exception:");
      error(t);
    }
  }

  @Override
  public void action(final SysEvent ev) throws NotificationException {
    if (ev == null) {
      return;
    }

    try {
      if (debug) {
        trace("Received message" + ev);
      }
      if (ev instanceof EntityQueuedEvent) {
        counts.total++;

        EntityQueuedEvent eqe = (EntityQueuedEvent)ev;

        for (int ct = 1; ct <= retryLimit; ct++) {
          if (ct - 1 > counts.maxRetries) {
            counts.maxRetries = ct - 1;
          }

          ProcessMessageResult pmr = handler.processMessage(eqe);

          if (pmr == ProcessMessageResult.PROCESSED) {
            counts.processed++;

            return;
          }

          if (pmr == ProcessMessageResult.NO_ACTION) {
            counts.noaction++;
            return;
          }

          if (pmr == ProcessMessageResult.STALE_STATE) {
            counts.staleState++;
            counts.retries++;

            if (ct == 1) {
              counts.retried++;
            }
          }

          if (pmr == ProcessMessageResult.FAILED_NORETRIES) {
            counts.failedNoRetries++;
            return;
          }

          if (pmr == ProcessMessageResult.FAILED) {
            counts.failed++;
            counts.retries++;

            if (ct == 1) {
              counts.retried++;
            }
          }
        }

        /* Failed after retries */
        counts.failedRetries++;
      }
    } catch (Throwable t) {
      error("Error processing message " + ev);
      error(t);
    }
  }

  /**
   * @param args
   */
  public static void main(final String[] args) {
    try {
      InoutSched sched = new InoutSched(new ScheduleMesssageCounts(""), 10);

      if (!sched.processArgs(new Args(args))) {
        return;
      }

      Thread insched = new Thread(new InoutSched(new ScheduleMesssageCounts("In"),
                                                 10, true));

      Thread outsched = new Thread(new InoutSched(new ScheduleMesssageCounts("Out"),
                                                  10, false));

      insched.run();
      outsched.run();

      insched.join();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}
