/*
 * Copyright 2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.workflow;

import azkaban.common.utils.Props;

/**
 * Basic unit of work.
 * 
 * @author Richard B Park
 */
public abstract class WorkUnit {
	private final String id;
	private Props prop;
	
	public WorkUnit(String id, WorkUnit toClone) {
		this.id = id;
		prop = Props.clone(toClone.getProps());
	}
	
	public WorkUnit(String id, Props prop) {
		this.id = id;
		this.prop = prop;
	}
	
	public String getId() {
		return id;
	}
	
	public Props getProps() {
		return prop;
	}
}
