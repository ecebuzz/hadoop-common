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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
//swm
import org.apache.hadoop.io.WritableUtils;
//mws

public class JvmTask implements Writable {
  Task t;
  //swm: generalize shouldDie to an action indicator
  //boolean shouldDie;
  enum JvmAction {Noop, Die, Change};
  JvmAction jvmAction;
  
  //swm: include the environment variable of this new task
  //JobID newJobId;
  
  //swm: reset the following two environment variables
  String cwd; // current working directory
  String jobTokenFile; // job token file
  //String logLocation; // log location
  //mws
  
  //swm
  /*
  public JvmTask(Task t, JvmAction ja, JobID jobId) {
  	this.t = t;
  	this.jvmAction = ja;
  	//this.newJobId = jobId;
  }*/
  public JvmTask(Task t, JvmAction ja, String cwd, String jobTokenFile) {
  	this.t = t;
  	this.jvmAction = ja;
  	this.cwd = cwd;
  	this.jobTokenFile = jobTokenFile;
  }
  public JvmTask(Task t, JvmAction ja, String cwd) {
  	this(t,ja,cwd,null);
  }
  
  public JvmTask(Task t, JvmAction ja) {
  	this(t,ja,null,null);
  }
  
  public JvmTask(Task t) {
  	this(t,JvmAction.Noop,null,null);
  }
  //mws
  
  /*swm
  public JvmTask(Task t, boolean shouldDie) {
    this.t = t;
    this.shouldDie = shouldDie;
  }
  */
  
  public JvmTask() {}
  public Task getTask() {
    return t;
  }
  
  /* swm
  public boolean shouldDie() {
    return shouldDie;
  }
  */
  
  public boolean shouldDie() {
  	return (jvmAction.equals(JvmAction.Die));
  }
  
  //swm
  public boolean shouldChange() {
  	return (jvmAction.equals(JvmAction.Change));
  }

  /*
  public JobID getNewJobID() {
  	return newJobId;
  }*/
  
  public String getNewWorkingDirectory() {
  	return cwd;
  }
  
  public String getNewJobTokenFile() {
  	return jobTokenFile;
  }

  /*  
  public String getNewLogLocation() {
  	return logLocation;
  } */
  //mws
 
  // swm:  incorporate a jvm action parameter: new job id to be bound to a jvm
/*  
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(shouldDie);
    if (t != null) {
      out.writeBoolean(true);
      out.writeBoolean(t.isMapTask());
      t.write(out);
    } else {
      out.writeBoolean(false);
    }
  }
*/
  public void write(DataOutput out) throws IOException {
    if (t != null) {
      out.writeBoolean(true);
      out.writeBoolean(t.isMapTask());
      t.write(out);
    } else {
      out.writeBoolean(false);
    }
		WritableUtils.writeEnum(out, jvmAction);
		WritableUtils.writeString(out, cwd);
		if (jvmAction == JvmAction.Change) {
			WritableUtils.writeString(out, jobTokenFile);
		}
 }
  //swm
  /*
  public void readFields(DataInput in) throws IOException {
    shouldDie = in.readBoolean();
    boolean taskComing = in.readBoolean();
    if (taskComing) {
      boolean isMap = in.readBoolean();
      if (isMap) {
        t = new MapTask();
      } else {
        t = new ReduceTask();
      }
      t.readFields(in);
    }
  }
  */
  public void readFields(DataInput in) throws IOException {
     boolean taskComing = in.readBoolean();
    if (taskComing) {
      boolean isMap = in.readBoolean();
      if (isMap) {
        t = new MapTask();
      } else {
        t = new ReduceTask();
      }
      t.readFields(in);
    }
    jvmAction = WritableUtils.readEnum(in, JvmAction.class);
    cwd = WritableUtils.readString(in);
    if (jvmAction == JvmAction.Change) {
    	jobTokenFile = WritableUtils.readString(in);
    }
  }
}
