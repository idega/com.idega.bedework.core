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

import org.apache.log4j.Logger;

/**
 * @author douglm
 *
 */
public class BwAlarmProc implements BwAlarmProcMBean {
  private transient Logger log;

  boolean debug;

  private class SysMessageThread extends Thread {
    private SystemMesssageHandler mproc;

    /**
     * @param name - for the thread
     * @param mproc
     */
    public SysMessageThread(final String name, final SystemMesssageHandler mproc) {
      super(name);

      this.mproc = mproc;
    }

    @Override
    public void run() {
      try {
        mproc.run();
      } catch (Throwable t) {
        error(t.getMessage());
      }
    }
  }

  private class ProcessorThread extends Thread {
    private AlarmHandler alarmHandler;

    /**
     * @param name - for the thread
     * @param alarmHandler
     */
    public ProcessorThread(final String name, final AlarmHandler alarmHandler) {
      super(name);

      this.alarmHandler = alarmHandler;
    }

    @Override
    public void run() {
      try {
        alarmHandler.run();
      } catch (Throwable t) {
        error(t.getMessage());
      }
    }
  }

  private String processorQueueName;

  private AlarmHandler alarmHandler;

  private SysMessageThread msgThread;
  private ProcessorThread processor;

  private String principal;

  /**
   *
   */
  public BwAlarmProc() {
    debug = getLogger().isDebugEnabled();
  }

  public void setPrincipal(final String val) {
    principal = val;
  }

  public String getPrincipal() {
    return principal;
  }

  /**
   * @param val
   */
  public void setProcessorQueueName(final String val) {
    processorQueueName = val;
  }

  /**
   * @return String processorQueueName we use
   */
  public String getProcessorQueueName() {
    return processorQueueName;
  }

  public long getProcessedCount() {
    return 0;
  }

  public String getNextAlarmTime() {
    return "";
  }

  public String getName() {
    /* This apparently must be the same as the name attribute in the
     * jboss service definition
     */
    return "org.bedework:service=BwAlarmProc";
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
    return (processor != null) && processor.isAlive();
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#start()
   */
  public synchronized void start() {
    alarmHandler = new AlarmHandler(getPrincipal());

    info("************************************************************");
    info(" * Starting " + getName());
    info("************************************************************");

    startMsgHandler();
    startProcessor();
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#stop()
   */
  public synchronized void stop() {
    stopMsgHandler();
    stopProcessor();
  }

  private void startMsgHandler() {
    if (msgThread != null) {
      error("Msg listener already started");
      return;
    }

    info("************************************************************");
    info(" * Starting msg listener " + getName());
    info("************************************************************");

    try {
      msgThread = new SysMessageThread(getName(),
                                       new SystemMesssageHandler(alarmHandler,
                                                                 processorQueueName));
    } catch (Throwable t) {
      error("Error starting msg listener");
      error(t);
    }

    processor.start();
  }

  private void startProcessor() {
    if (processor != null) {
      error("Alarm processor already started");
      return;
    }

    info("************************************************************");
    info(" * Starting processor " + getName());
    info("************************************************************");

    try {
      processor = new ProcessorThread(getName(), alarmHandler);
    } catch (Throwable t) {
      error("Error starting alarm processor");
      error(t);
    }

    processor.start();
  }

  private void stopMsgHandler() {
    if (msgThread == null) {
      error("Alarm msg listener already stopped");
      return;
    }

    info("************************************************************");
    info(" * Stopping msg listener " + getName());
    info("************************************************************");

    stopProc(msgThread);
    msgThread = null;

    info("************************************************************");
    info(" * msg listener " + getName() + " terminated");
    info("************************************************************");
  }

  private void stopProcessor() {
    if (processor == null) {
      error("Alarm processor already stopped");
      return;
    }

    info("************************************************************");
    info(" * Stopping processor " + getName());
    info("************************************************************");

    processor.alarmHandler.stop();
    stopProc(processor);
    processor = null;

    info("************************************************************");
    info(" * processor " + getName() + " terminated");
    info("************************************************************");
  }

  private void stopProc(final Thread p) {
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

  protected void trace(final String msg) {
    getLogger().debug(msg);
  }
}
