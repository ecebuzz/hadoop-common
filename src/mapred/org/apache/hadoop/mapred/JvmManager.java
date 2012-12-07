/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
//swm
import java.util.AbstractMap;
//mws

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.JvmFinishedEvent;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.ProcessTree.Signal;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

class JvmManager {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.JvmManager");

  private JvmManagerForType mapJvmManager;

  private JvmManagerForType reduceJvmManager;
  
  public JvmEnv constructJvmEnv(List<String> setup, Vector<String>vargs,
      File stdout,File stderr,long logSize, File workDir, 
      JobConf conf) {
    return new JvmEnv(setup,vargs,stdout,stderr,logSize,workDir,conf);
  }
  
  public JvmManager(TaskTracker tracker) {
    mapJvmManager = new JvmManagerForType(tracker.getMaxCurrentMapTasks(), 
        true, tracker);
    reduceJvmManager = new JvmManagerForType(tracker.getMaxCurrentReduceTasks(),
        false, tracker);
  }

  //called from unit tests
  JvmManagerForType getJvmManagerForType(TaskType type) {
    if (type.equals(TaskType.MAP)) {
      return mapJvmManager;
    } else if (type.equals(TaskType.REDUCE)) {
      return reduceJvmManager;
    }
    return null;
  }
  
  /*
   * Saves pid of the given taskJvm
   */
  void setPidToJvm(JVMId jvmId, String pid) {
    if (jvmId.isMapJVM()) {
      mapJvmManager.jvmIdToPid.put(jvmId, pid);
    }
    else {
      reduceJvmManager.jvmIdToPid.put(jvmId, pid);
    }
  }
  
  /*
   * Returns the pid of the task
   */
  String getPid(TaskRunner t) {
    if (t != null && t.getTask() != null) {
      if (t.getTask().isMapTask()) {
        JVMId id = mapJvmManager.runningTaskToJvm.get(t);
        if (id != null) {
          return mapJvmManager.jvmIdToPid.get(id);
        }
      } else {
        JVMId id = reduceJvmManager.runningTaskToJvm.get(t);
        if (id != null) {
          return reduceJvmManager.jvmIdToPid.get(id);
        }
      }
    }
    return null;
  }
  
  
  public void stop() throws IOException, InterruptedException {
    mapJvmManager.stop();
    reduceJvmManager.stop();
  }

  public boolean isJvmKnown(JVMId jvmId) {
    if (jvmId.isMapJVM()) {
      return mapJvmManager.isJvmknown(jvmId);
    } else {
      return reduceJvmManager.isJvmknown(jvmId);
    }
  }

  public void launchJvm(TaskRunner t, JvmEnv env
                        ) throws IOException, InterruptedException {
    if (t.getTask().isMapTask()) {
      mapJvmManager.reapJvm(t, env);
    } else {
      reduceJvmManager.reapJvm(t, env);
    }
  }

  public boolean validateTipToJvm(TaskInProgress tip, JVMId jvmId) {
    if (jvmId.isMapJVM()) {
      return mapJvmManager.validateTipToJvm(tip, jvmId);
    } else {
      return reduceJvmManager.validateTipToJvm(tip, jvmId);
    }
  }

  public TaskInProgress getTaskForJvm(JVMId jvmId)
      throws IOException {
    if (jvmId.isMapJVM()) {
      return mapJvmManager.getTaskForJvm(jvmId);
    } else {
      return reduceJvmManager.getTaskForJvm(jvmId);
    }
  }
  public void taskFinished(TaskRunner tr) {
    if (tr.getTask().isMapTask()) {
      mapJvmManager.taskFinished(tr);
    } else {
      reduceJvmManager.taskFinished(tr);
    }
  }

  public void taskKilled(TaskRunner tr
                         ) throws IOException, InterruptedException {
    if (tr.getTask().isMapTask()) {
      mapJvmManager.taskKilled(tr);
    } else {
      reduceJvmManager.taskKilled(tr);
    }
  }

  //swm
  public boolean shouldChangeJvmBinding(JVMId jvmId) {
  	LOG.info("Querry about the need to change the jvm and job binding" + jvmId);
  	if(jvmId.isMap) {
  		return mapJvmManager.jvmChangeList.containsKey(jvmId);
  	} else {
  		return reduceJvmManager.jvmChangeList.containsKey(jvmId);
  	}
  }
  
  public JobID getNewJobId(JVMId oldJvmId) {
  	LOG.info("Get the new job id for the jvm " + oldJvmId);
  	if(oldJvmId.isMap) {
  		return mapJvmManager.jvmChangeList.get(oldJvmId).getValue();
  	} else {
  		return reduceJvmManager.jvmChangeList.get(oldJvmId).getValue();
  	}  	
  }
  
  public JVMId getNewJvmId(JVMId oldJvmId) {
  	LOG.info("Get the new jvm id for jvm " + oldJvmId);
  	if(oldJvmId.isMap) {
  		return mapJvmManager.jvmChangeList.get(oldJvmId).getKey();
  	} else {
  		return reduceJvmManager.jvmChangeList.get(oldJvmId).getKey();
  	}  	
  }  
  //mws
  
  
  public void killJvm(JVMId jvmId) throws IOException, InterruptedException {
	//swm
	  LOG.info("swmlog: Kill Jvm " + jvmId);
	//mws
    if (jvmId.isMap) {
      mapJvmManager.killJvm(jvmId);
    } else {
      reduceJvmManager.killJvm(jvmId);
    }
  }
  
//  //swm
//  public void registerJvm(JVMId jvmId , JobID jobId) {
//  	LOG.info("swmlog: Unregister Jvm " + jvmId);
//  	if (jvmId.isMap) {
//  		mapJvmManager.unregisterJvm(jvmId);
//  	} else {
//  		reduceJvmManager.unregisterJvm(jvmId);
//  	}
//  }
  
  public void unregisterJvm(JVMId jvmId) {
  	LOG.info("swmlog: Unregister Jvm " + jvmId);
  	if (jvmId.isMap) {
  		mapJvmManager.unregisterJvm(jvmId);
  	} else {
  		reduceJvmManager.unregisterJvm(jvmId);
  	}
  }
  
  public void removeFromChangeList(JVMId jvmId) {
  	LOG.info("swmlog: remove old Jvm " +  jvmId + " from the change list");
  	if (jvmId.isMap) {
  		mapJvmManager.removeFromChangeList(jvmId);
  	} else {
  		reduceJvmManager.removeFromChangeList(jvmId);
  	}
  }  
  //mws

  /**
   * Adds the task's work dir to the cleanup queue of taskTracker for
   * asynchronous deletion of work dir.
   * @param tracker taskTracker
   * @param task    the task whose work dir needs to be deleted
   */
  static void deleteWorkDir(TaskTracker tracker, Task task) {
    String user = task.getUser();
    String jobid = task.getJobID().toString();
    String taskid = task.getTaskID().toString();
    String workDir = TaskTracker.getTaskWorkDir(user, jobid, taskid, 
                                                task.isTaskCleanupTask());
    String userDir = TaskTracker.getUserDir(user);
    tracker.getCleanupThread().addToQueue(
     new TaskController.DeletionContext(tracker.getTaskController(), false,
                                        user, 
                                        workDir.substring(userDir.length())));
                                           
  }
  
  static class JvmManagerForType {
    //Mapping from the JVM IDs to running Tasks
    Map <JVMId,TaskRunner> jvmToRunningTask = 
      new HashMap<JVMId, TaskRunner>();
    //Mapping from the tasks to JVM IDs
    Map <TaskRunner,JVMId> runningTaskToJvm = 
      new HashMap<TaskRunner, JVMId>();
    //Mapping from the JVM IDs to Reduce JVM processes
    Map <JVMId, JvmRunner> jvmIdToRunner = 
      new HashMap<JVMId, JvmRunner>();
    //Mapping from the JVM IDs to process IDs
    Map <JVMId, String> jvmIdToPid = 
      new HashMap<JVMId, String>();
    
    //swm
    private final boolean jvmCacheEnabled;
    private boolean changeOldJvm;
    private JobID newJobId;
    private Map<JVMId, Map.Entry<JVMId,JobID>> jvmChangeList;
    //mws
    int maxJvms;
    boolean isMap;
    private final long sleeptimeBeforeSigkill;
    
    Random rand = new Random(System.currentTimeMillis());
    static final String DELAY_BEFORE_KILL_KEY =
      "mapred.tasktracker.tasks.sleeptime-before-sigkill";
    // number of milliseconds to wait between TERM and KILL.
    private static final long DEFAULT_SLEEPTIME_BEFORE_SIGKILL = 250;
    private TaskTracker tracker;

    public JvmManagerForType(int maxJvms, boolean isMap, 
        TaskTracker tracker) {
      this.maxJvms = maxJvms;
      this.isMap = isMap;
      this.tracker = tracker;
      sleeptimeBeforeSigkill =
        tracker.getJobConf().getLong(DELAY_BEFORE_KILL_KEY,
                                     DEFAULT_SLEEPTIME_BEFORE_SIGKILL);
      //swm
      jvmCacheEnabled = tracker.getJobConf().getJvmCacheEnabled();
      changeOldJvm = false;
      newJobId = null;
      jvmChangeList = new HashMap<JVMId, Map.Entry<JVMId,JobID>>();
      //mws
    }

    synchronized public void setRunningTaskForJvm(JVMId jvmId, 
        TaskRunner t) {
      jvmToRunningTask.put(jvmId, t);
      runningTaskToJvm.put(t,jvmId);
      //swm
      if (jvmIdToRunner.get(jvmId) == null ) {
      	LOG.debug("Jvm " + jvmId + " does not exist in jvmIdToRunner");
      } else {
      	jvmIdToRunner.get(jvmId).setBusy(true);
      }
      /*
      try {
      	jvmIdToRunner.get(jvmId).setBusy(true);
      } catch (NullPointerException e) {
      	for (Map.Entry<JVMId, JvmRunner> jvm_info : jvmIdToRunner.entrySet() ) {
      		LOG.debug("swmlog: JvmId: " + jvm_info.getKey() + " JvmRunner: " + jvm_info.getValue());      		
      	}
      }
      */
      //mws
    }
    
    synchronized public boolean validateTipToJvm(TaskInProgress tip, JVMId jvmId) {
      if (jvmId == null) {
        LOG.warn("Null jvmId. Cannot verify Jvm. validateTipToJvm returning false");
        return false;
      }
      TaskRunner taskRunner = jvmToRunningTask.get(jvmId);
      if (taskRunner == null) {
        return false; //JvmId not known.
      }
      TaskInProgress knownTip = taskRunner.getTaskInProgress();
      if (knownTip == tip) { // Valid to compare the addresses ? (or equals)
        return true;
      } else {
        return false;
      }
    }

    synchronized public TaskInProgress getTaskForJvm(JVMId jvmId)
        throws IOException {
      if (jvmToRunningTask.containsKey(jvmId)) {
        //Incase of JVM reuse, tasks are returned to previously launched
        //JVM via this method. However when a new task is launched
        //the task being returned has to be initialized.
        TaskRunner taskRunner = jvmToRunningTask.get(jvmId);
        JvmRunner jvmRunner = jvmIdToRunner.get(jvmId);
        Task task = taskRunner.getTaskInProgress().getTask();

        jvmRunner.taskGiven(task);
        return taskRunner.getTaskInProgress();

      }
      return null;
    }
    
    synchronized public boolean isJvmknown(JVMId jvmId) {
      return jvmIdToRunner.containsKey(jvmId);
    }

    synchronized public void taskFinished(TaskRunner tr) {
      JVMId jvmId = runningTaskToJvm.remove(tr);
      if (jvmId != null) {
        jvmToRunningTask.remove(jvmId);
        JvmRunner jvmRunner;
        if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null) {
          jvmRunner.taskRan();
        }
      }
    }

    synchronized public void taskKilled(TaskRunner tr
                                        ) throws IOException,
                                                 InterruptedException {
      JVMId jvmId = runningTaskToJvm.remove(tr);
      if (jvmId != null) {
        jvmToRunningTask.remove(jvmId);
        killJvm(jvmId);
      }
    }

    synchronized public void killJvm(JVMId jvmId) throws IOException, 
                                                         InterruptedException {
      JvmRunner jvmRunner;
      if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null) {
        killJvmRunner(jvmRunner);
      }
    }
    
    //swm
    synchronized public void unregisterJvm(JVMId jvmId) {
    	if (jvmIdToRunner.containsKey(jvmId)) {
    		removeJvm(jvmId);
    	}
    }
    
    synchronized public void removeFromChangeList(JVMId jvmId) {
    	if (jvmChangeList.containsKey(jvmId)) {
    		jvmChangeList.remove(jvmId);
    	}
    }
    //mws
    
    synchronized public void stop() throws IOException, InterruptedException {
      //since the kill() method invoked later on would remove
      //an entry from the jvmIdToRunner map, we create a
      //copy of the values and iterate over it (if we don't
      //make a copy, we will encounter concurrentModification
      //exception
      List <JvmRunner> list = new ArrayList<JvmRunner>();
      list.addAll(jvmIdToRunner.values());
      for (JvmRunner jvm : list) {
        killJvmRunner(jvm);
      }
    }

    private synchronized void killJvmRunner(JvmRunner jvmRunner
                                            ) throws IOException,
                                                     InterruptedException {
      jvmRunner.kill();
      removeJvm(jvmRunner.jvmId);
    }

    synchronized private void removeJvm(JVMId jvmId) {
      jvmIdToRunner.remove(jvmId);
      jvmIdToPid.remove(jvmId);
    }
    private synchronized void reapJvm( 
        TaskRunner t, JvmEnv env) throws IOException, InterruptedException {
      if (t.getTaskInProgress().wasKilled()) {
        //the task was killed in-flight
        //no need to do the rest of the operations
        return;
      }
      boolean spawnNewJvm = false;

      JobID jobId = t.getTask().getJobID();
      //Check whether there is a free slot to start a new JVM.
      //,or, Kill a (idle) JVM and launch a new one
      //When this method is called, we *must* 
      // (1) spawn a new JVM (if we are below the max) 
      // (2) find an idle JVM (that belongs to the same job), or,
      // swm
      // (4) reuse (but not kill ) an idle JVM of a different job with reusable cache
      // mws
      // (3) kill an idle JVM (from a different job) //swm if the cached is not reusable //mws
      // (the order of return is in the order above)
      int numJvmsSpawned = jvmIdToRunner.size();
      //swm
      LOG.info("swmlog: the number of spawned "
      		+ (isMap ? " MapJvm " : " ReduceJvm")
      		+ "jvms is " + numJvmsSpawned);
      //mws
      JvmRunner runnerToKill = null;
      
      //swm: change the mapping between job and jvm
      JVMId jvmIdToChange = null;
      //mws

      if (numJvmsSpawned >= maxJvms) {         
        //go through the list of JVMs for all jobs.
      	//swm
        Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = 
          jvmIdToRunner.entrySet().iterator();
        
        while (jvmIter.hasNext()) {
          
        	//swm JvmRunner jvmRunner = jvmIter.next().getValue(); //mws
        	Map.Entry<JVMId, JvmRunner> entry = jvmIter.next();
        	JvmRunner jvmRunner = entry.getValue();
      	
        	//mws
          JobID jId = jvmRunner.jvmId.getJobId();
          //look for a free JVM for this job; if one exists then just break
          if (jId.equals(jobId) && !jvmRunner.isBusy() && !jvmRunner.ranAll()){
          	setRunningTaskForJvm(jvmRunner.jvmId, t); //reserve the JVM
            LOG.info("No new JVM spawned for jobId/taskid: " + 
                     jobId+"/"+t.getTask().getTaskID() +
                     ". Attempting to reuse: " + jvmRunner.jvmId
                     //swm
                     + " pid " + jvmIdToPid.get(jvmRunner.jvmId));
            		 		 //mws
            return;
          }
          // swm (4)
					if (jvmCacheEnabled) {
					//if (t.getTask().isMapTask() && jvmCacheEnabled) {
						// favor reusing map jvms of different jobs that enable cache
						// check whether it is possible to reuse jvm of a different job
						// 1) the task is from a different job
						// 2) jvm is not busy
						// 3) jvm has not reached its maximum number of tasks to execute
						// 4) jvm cache is reusable
						if (!jId.equals(jobId) && !jvmRunner.isBusy()
								&& !jvmRunner.ranAll() && jvmRunner.isJvmCacheReusable()) {
							// change the job id of this jvm to the new job id
							jvmIdToChange = entry.getKey();

							//To do: remove the old jvmRunner, add this new jvmRunner back
							// change the data structure of JVMId to reflect the change of the mapping from job to jvm

							changeOldJvm = true;
							spawnNewJvm = false;
							newJobId = jobId;
							LOG.debug("swmlog: changeOldJvm is set to " + changeOldJvm);
							break;
						}
					}
					// mws
       
          //Cases when a JVM is killed: 
          // (1) the JVM under consideration belongs to the same job 
          //     (passed in the argument). In this case, kill only when
          //     the JVM ran all the tasks it was scheduled to run (in terms
          //     of count).
          // (2) the JVM under consideration belongs to a different job and is
          //     currently not busy //swm and the jvm cache is not reusable //mws
          //But in both the above cases, we see if we can assign the current
          //task to an idle JVM (hence we continue the loop even on a match)
          
          if ((jId.equals(jobId) && jvmRunner.ranAll()) ||
              //swm (!jId.equals(jobId) && !jvmRunner.isBusy())) { //mws
        	 (!jId.equals(jobId) && !jvmRunner.isBusy() && !jvmRunner.isJvmCacheReusable())) {
            runnerToKill = jvmRunner;
            spawnNewJvm = true;
          }
       }

      } else {
        spawnNewJvm = true;
      }

			// swm
			if (changeOldJvm) {
				assert (jvmIdToChange != null);
				LOG.info("swmlog: try to update the data structure on TT side");
				changeOldJvm(jvmIdToChange, jobId, env, t);
				return;
			}
			// mws

      if (spawnNewJvm) {
        if (runnerToKill != null) {	
          //swm
          //LOG.info("Killing JVM: " + runnerToKill.jvmId);
          LOG.info("swmlog: Killing JVM: " + runnerToKill.jvmId + 
        		  " pid " + jvmIdToPid.get(runnerToKill.jvmId) );
          killJvmRunner(runnerToKill);
        }
        spawnNewJvm(jobId, env, t);
        return;
      }
      //*MUST* never reach this
      LOG.fatal("Inconsistent state!!! " +
      		"JVM Manager reached an unstable state " +
            "while reaping a JVM for task: " + t.getTask().getTaskID()+
            " " + getDetails() + ". Aborting. ");
      System.exit(-1);
    }
    
    private String getDetails() {
      StringBuffer details = new StringBuffer();
      details.append("Number of active JVMs:").
              append(jvmIdToRunner.size());
      Iterator<JVMId> jvmIter = 
        jvmIdToRunner.keySet().iterator();
      while (jvmIter.hasNext()) {
        JVMId jvmId = jvmIter.next();
        details.append("\n  JVMId ").
          append(jvmId.toString()).
          append(" #Tasks ran: "). 
          append(jvmIdToRunner.get(jvmId).numTasksRan).
          append(" Currently busy? ").
          append(jvmIdToRunner.get(jvmId).busy).
          append(" Currently running: "). 
          append(jvmToRunningTask.get(jvmId).getTask().getTaskID().toString());
			}
			return details.toString();
		}

		private void spawnNewJvm(JobID jobId, JvmEnv env, TaskRunner t) {
			JvmRunner jvmRunner = new JvmRunner(env, jobId, t.getTask());
			jvmIdToRunner.put(jvmRunner.jvmId, jvmRunner);
			// spawn the JVM in a new thread. Note that there will be very little
			// extra overhead of launching the new thread for a new JVM since
			// most of the cost is involved in launching the process. Moreover,
			// since we are going to be using the JVM for running many tasks,
			// the thread launch cost becomes trivial when amortized over all
			// tasks. Doing it this way also keeps code simple.
			jvmRunner.setDaemon(true);
			jvmRunner.setName("JVM Runner " + jvmRunner.jvmId + " spawned.");
			setRunningTaskForJvm(jvmRunner.jvmId, t);
			LOG.info(jvmRunner.getName());
			jvmRunner.start();
		}

		private void changeOldJvm(JVMId jvmIdToChange, JobID jobId, JvmEnv env,
				TaskRunner t) {
			JVMId oldJvmId = new JVMId(jvmIdToChange);
			LOG.info("swmlog: oldJvmId: " + jvmIdToChange);
			// change the job id of JVMId and JvmRunner

			if (jvmIdToRunner.containsKey(jvmIdToChange)) {
				// swm: the key of jvmIdToRunner is a mutable type
				// change the value of the key does not change its location in the Map
				// we have to remove, modify, and add it (overhead?)

				JvmRunner jvmRunner = jvmIdToRunner.remove(jvmIdToChange);
				jvmRunner.jvmId.setJobId(jobId);
				LOG.info("swmlog: Change the job id of Jvm " + oldJvmId + " to "
						+ jvmIdToChange);
				jvmRunner.jobIds.add(jobId);
				jvmRunner.currentJobIdIndex++;
				//jvmRunner.firstTask = t.getTask();

				// reuse the JVM
				// to do: need to change the Child jvm part to update the change of
				// JvmId
				jvmIdToRunner.put(jvmRunner.jvmId, jvmRunner);
				setRunningTaskForJvm(jvmRunner.jvmId, t);

				// record the mapping from the old jvm id to the new jvm id
				jvmChangeList.put(oldJvmId, new AbstractMap.SimpleEntry<JVMId, JobID>(
						jvmIdToChange, jobId));

				LOG.info("swmlog: Jvm cache is enabled. No new JVM spawned for jobId/taskid: "
						+ jobId
						+ "/"
						+ t.getTask().getTaskID()
						+ ". Attempting to reuse jvm: " + jvmRunner.jvmId);
				//jvmRunner.start();

			} else {
				LOG.info("Error: jvmIdToRunner does not contain oldJvmId "
						+ jvmIdToChange);
			}
		}
    
    synchronized private void updateOnJvmExit(JVMId jvmId, 
        int exitCode) {
      removeJvm(jvmId);
      TaskRunner t = jvmToRunningTask.remove(jvmId);

      if (t != null) {
        runningTaskToJvm.remove(t);
        if (exitCode != 0) {
          t.setExitCode(exitCode);
        }
        t.signalDone();
      }
    }

    class JvmRunner extends Thread {
      JvmEnv env;
      volatile boolean killed = false;   
      volatile int numTasksRan;
      //added by swm
      volatile int numTasksRanForOriginalJob;
      //end of add
      
      final int numTasksToRun;

      
      JVMId jvmId;
      volatile boolean busy = true;
      //swm
      Vector<JobID> jobIds;
      int currentJobIdIndex;
      volatile boolean isJvmCacheReusable = jvmCacheEnabled;
      // mws
      private ShellCommandExecutor shexec; // shell terminal for running the task
      private Task firstTask;

      private List<Task> tasksGiven = new ArrayList<Task>();

      void taskGiven(Task task) {
        tasksGiven.add(task);
      }

      public JvmRunner(JvmEnv env, JobID jobId, Task firstTask) {
        this.env = env;
        //added by swm
        jobIds = new Vector<JobID>();
        jobIds.add(jobId);
        currentJobIdIndex = 0;
        //end of add
        this.jvmId = new JVMId(jobId, isMap, rand.nextInt());
        this.numTasksToRun = env.conf.getNumTasksToExecutePerJvm();
        this.firstTask = firstTask;
        LOG.info("In JvmRunner constructed JVM ID: " + jvmId);
      }

      @Override
      public void run() {
        try {
          runChild(env);
        } catch (InterruptedException ie) {
          return;
        } catch (IOException e) {
          LOG.warn("Caught IOException in JVMRunner", e);
        } catch (Throwable e) {
          LOG.error("Caught Throwable in JVMRunner. Aborting TaskTracker.", e);
          System.exit(1);
        } finally {
          jvmFinished();
        }
      }

      public void runChild(JvmEnv env) throws IOException, InterruptedException{
        int exitCode = 0;
        try {
          env.vargs.add(Integer.toString(jvmId.getId()));
          TaskRunner runner = jvmToRunningTask.get(jvmId);
          if (runner != null) {
            Task task = runner.getTask();
            //Launch the task controller to run task JVM
            String user = task.getUser();
            TaskAttemptID taskAttemptId = task.getTaskID();
            String taskAttemptIdStr = task.isTaskCleanupTask() ? 
                (taskAttemptId.toString() + TaskTracker.TASK_CLEANUP_SUFFIX) :
                  taskAttemptId.toString(); 
                exitCode = tracker.getTaskController().launchTask(user,
                    jvmId.jobId.toString(), taskAttemptIdStr, env.setup,
                    env.vargs, env.workDir, env.stdout.toString(),
                    env.stderr.toString());
          }
        } catch (IOException ioe) {
          // do nothing
          // error and output are appropriately redirected
        } finally { // handle the exit code
          // although the process has exited before we get here,
          // make sure the entire process group has also been killed.
        	
        	//swm
        	if (!isJvmCacheReusable()) {
        	//mws
	          kill();
	          updateOnJvmExit(jvmId, exitCode);
	          LOG.info("JVM : " + jvmId + " exited with exit code " + exitCode
	              + ". Number of tasks it ran: " + numTasksRan);
	          deleteWorkDir(tracker, firstTask);
	        //swm
        	} else {
        		LOG.info("swmlog: Jvm cached is enabled, Jvm " + jvmId + " is kept alive in runChild.");
        		//BUG? deleteWorkDir(tracker, firstTask);
        	}
        	//mws
        }
      }

      private class DelayedProcessKiller extends Thread {
        private final String user;
        private final int pid;
        private final long delay;
        private final Signal signal;
        DelayedProcessKiller(String user, int pid, long delay, Signal signal) {
          this.user = user;
          this.pid = pid;
          this.delay = delay;
          this.signal = signal;
          setName("Task killer for " + pid);
          setDaemon(false);
        }
        @Override
        public void run() {
          try {
            Thread.sleep(delay);
            tracker.getTaskController().signalTask(user, pid, signal);
          } catch (InterruptedException e) {
            return;
          } catch (IOException e) {
            LOG.warn("Exception when killing task " + pid, e);
          }
        }
      }

      synchronized void kill() throws IOException, InterruptedException {
        if (!killed) {
          TaskController controller = tracker.getTaskController();
          // Check inital context before issuing a kill to prevent situations
          // where kill is issued before task is launched.
          String pidStr = jvmIdToPid.get(jvmId);
          if (pidStr != null) {
            String user = env.conf.getUser();
            int pid = Integer.parseInt(pidStr);
            // start a thread that will kill the process dead
            if (sleeptimeBeforeSigkill > 0) {
              new DelayedProcessKiller(user, pid, sleeptimeBeforeSigkill, 
                                       Signal.KILL).start();
              controller.signalTask(user, pid, Signal.TERM);
            } else {
              controller.signalTask(user, pid, Signal.KILL);
            }
          } else {
            LOG.info(String.format("JVM Not killed %s but just removed", jvmId
                .toString()));
          }
          killed = true;
        }
      }

      // Post-JVM-exit logs processing. inform user log manager
      private void jvmFinished() {
        JvmFinishedEvent jfe = new JvmFinishedEvent(new JVMInfo(
            TaskLog.getAttemptDir(firstTask.getTaskID(), 
                                  firstTask.isTaskCleanupTask()),
            tasksGiven));
        tracker.getUserLogManager().addLogEvent(jfe);
      }

      public void taskRan() {
        busy = false;
        numTasksRan++;
        // added by swm
//        if(this.) {
//        	numTasksRanForOriginalJob++;
//        }
        // end of add
      }
      
      public boolean ranAll() {
        return(numTasksRan == numTasksToRun);
      }
      //swm
      public boolean isJvmCacheReusable() {
    	  //To do: add the logic to determine whether it is possible to reuse cache
    	  //add a private member to the JvmRunner
    	  return isJvmCacheReusable;
      }
      //mws
      public void setBusy(boolean busy) {
        this.busy = busy;
      }
      public boolean isBusy() {
        return busy;
      }
    }
  }  
  static class JvmEnv { //Helper class
    List<String> vargs;
    List<String> setup;
    File stdout;
    File stderr;
    File workDir;
    long logSize;
    JobConf conf;
    Map<String, String> env;

    public JvmEnv(List <String> setup, Vector<String> vargs, File stdout, 
        File stderr, long logSize, File workDir, JobConf conf) {
      this.setup = setup;
      this.vargs = vargs;
      this.stdout = stdout;
      this.stderr = stderr;
      this.workDir = workDir;
      this.conf = conf;
    }
  }
}
