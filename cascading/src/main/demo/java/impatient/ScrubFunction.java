/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package impatient;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class ScrubFunction extends BaseOperation implements Function
  {
  public ScrubFunction( Fields fieldDeclaration )
    {
    super( 2, fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String doc_id = argument.getString( 0 );
    String token = scrubText( argument.getString( 1 ) );

    if( token.length() > 0 )
      {
      Tuple result = new Tuple();
      result.add( doc_id );
      result.add( token );
      functionCall.getOutputCollector().add( result );
      }
    }

  public String scrubText( String text )
    {
    return text.trim().toLowerCase();
    }
  }