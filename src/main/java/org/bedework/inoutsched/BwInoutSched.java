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

import org.apache.log4j.Logger;

/**
 * @author douglm
 *
 */
public class BwInoutSched /*extends InoutSched*/ implements BwInoutSchedMBean {
  private class ProcessorThread extends Thread {
    private InoutSched sched;

    /**
     * @param name - for the thread
     * @param sched
     */
    public ProcessorThread(final String name, final InoutSched sched) {
      super(name);

      this.sched = sched;
    }

    @Override
    public void run() {
      try {
        sched.run();
      } catch (Throwable t) {
        error(t.getMessage());
      }
    }
  }

  private transient Logger log;

  private ProcessorThread inProcessor;
  private ProcessorThread outProcessor;

  private int incomingRetryLimit = 10;

  private int outgoingRetryLimit = 10;

  private Counts counts = new Counts();

  public void setIncomingRetryLimit(final int val) {
    incomingRetryLimit = val;

    if (inProcessor != null) {
      inProcessor.sched.setRetryLimit(val);
    }
  }

  public int getIncomingRetryLimit() {
    return incomingRetryLimit;
  }

  public void setOutgoingRetryLimit(final int val) {
    outgoingRetryLimit = val;

    if (outProcessor != null) {
      outProcessor.sched.setRetryLimit(val);
    }
  }

  public int getOutgoingRetryLimit() {
    return outgoingRetryLimit;
  }

  public Counts getCounts() {
    return counts;
  }

  public String getName() {
    /* This apparently must be the same as the name attribute in the
     * jboss service definition
     */
    return "org.bedework:service=BwInoutSched";
  }

  /* an example say's we need this  - we'll see
  public MBeanInfo getMBeanInfo() throws Exception {
    InitialContext ic = new InitialContext();
    RMIAdaptor server = (RMIAdaptor) ic.lookup("jmx/rmi/RMIAdaptor");

    ObjectName name = new ObjectName(MBEAN_OBJ_NAME);

    // Get the MBeanInfo for this MBean
    MBeanInfo info = server.getMBeanInfo(name);
    return info;
  }
  */

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#isStarted()
   */
  public boolean isStarted() {
    return (outProcessor != null) && outProcessor.isAlive();
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#start()
   */
  public synchronized void start() {
    if (outProcessor != null) {
      error("Already started");
      return;
    }

    info("************************************************************");
    info(" * Starting " + getName());
    info("************************************************************");

    try {
      inProcessor = new ProcessorThread(getName(),
                                        new InoutSched(counts.inCounts,
                                                       incomingRetryLimit,
                                                       true));
      outProcessor = new ProcessorThread(getName(),
                                         new InoutSched(counts.outCounts,
                                                        outgoingRetryLimit,
                                                        false));
    } catch (Throwable t) {
      error("Error starting scheduler");
      error(t);
    }

    inProcessor.start();
    outProcessor.start();
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#stop()
   */
  public synchronized void stop() {
    if (outProcessor == null) {
      error("Already stopped");
      return;
    }

    info("************************************************************");
    info(" * Stopping " + getName());
    info("************************************************************");

    stopProc(inProcessor);
    inProcessor = null;

    stopProc(outProcessor);
    outProcessor = null;

    info("************************************************************");
    info(" * " + getName() + " terminated");
    info("************************************************************");
  }

  private void stopProc(final ProcessorThread p) {
    if (p == null) {
      return;
    }

    p.interrupt();
    try {
      p.join();
    } catch (InterruptedException ie) {
    } catch (Throwable t) {
      error("Error waiting for processor termination");
      error(t);
    }
  }

  /* Get a logger for messages
   */
  protected Logger getLogger() {
    if (log == null) {
      log = Logger.getLogger(this.getClass());
    }

    return log;
  }

  protected void error(final Throwable t) {
    getLogger().error(this, t);
  }

  protected void error(final String msg) {
    getLogger().error(msg);
  }

  protected void info(final String msg) {
    getLogger().info(msg);
  }
}
