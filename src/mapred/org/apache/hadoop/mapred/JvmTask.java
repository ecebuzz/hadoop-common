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
  JobID newJobId;
  //mws
  
  //swm
  public JvmTask(Task t, JvmAction ja, JobID jobId) {
  	this.t = t;
  	this.jvmAction = ja;
  	this.newJobId = jobId;
  }
  
  public JvmTask(Task t, JvmAction ja) {
  	this.t = t;
  	this.jvmAction = ja;
  	this.newJobId = new JobID();
  }
  
  public JvmTask(Task t) {
  	this.t = t;
  	this.jvmAction = JvmAction.Noop;
  	this.newJobId = new JobID();
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
  
  public JobID getNewJobID() {
  	return newJobId;
  }
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
  	WritableUtils.writeEnum(out, jvmAction);
    if (newJobId != null) {
    	out.writeBoolean(true);
    	newJobId.write(out);
    } else {
    	out.writeBoolean(false);
    }
    if (t != null) {
      out.writeBoolean(true);
      out.writeBoolean(t.isMapTask());
      t.write(out);
    } else {
      out.writeBoolean(false);
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
  	jvmAction = WritableUtils.readEnum(in, JvmAction.class);
    boolean newJobComing = in.readBoolean();
    if (newJobComing) {
    	newJobId = JobID.read(in);
    	//newJobId.readFields(in);
    }
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
}
