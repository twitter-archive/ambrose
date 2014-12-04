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

import java.util.Properties;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Insert;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.twitter.ambrose.cascading.EmbeddedAmbroseCascadingNotifier;


public class Main{

  public static void main( String[] args ){
    String docPath = "src/test/data/rain.txt";
    String wcPath = "output/out";
    String stopPath = "src/test/data/en.stop";
    String tfidfPath = "output/out2";

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create source and sink taps
    Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
    Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

    Fields stop = new Fields( "stop" );
    Tap stopTap = new Hfs( new TextDelimited( stop, true, "\t" ), stopPath );
    Tap tfidfTap = new Hfs( new TextDelimited( true, "\t" ), tfidfPath );

    // specify a regex operation to split the "document" text lines into a token stream
    Fields token = new Fields( "token" );
    Fields text = new Fields( "text" );
    RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
    Fields fieldSelector = new Fields( "doc_id", "token" );
    Pipe docPipe = new Each( "token", text, splitter, fieldSelector );

    // define "ScrubFunction" to clean up the token stream
    Fields scrubArguments = new Fields( "doc_id", "token" );
    docPipe = new Each( docPipe, scrubArguments, new ScrubFunction( scrubArguments ), Fields.RESULTS );

    // perform a left join to remove stop words, discarding the rows
    // which joined with stop words, i.e., were non-null after left join
    Pipe stopPipe = new Pipe( "stop" );
    Pipe tokenPipe = new HashJoin( docPipe, token, stopPipe, stop, new LeftJoin() );
    tokenPipe = new Each( tokenPipe, stop, new RegexFilter( "^$" ) );
    tokenPipe = new Retain( tokenPipe, fieldSelector );

    // one branch of the flow tallies the token counts for term frequency (TF)
    Pipe tfPipe = new Pipe( "TF", tokenPipe );
    Fields tf_count = new Fields( "tf_count" );
    tfPipe = new CountBy( tfPipe, new Fields( "doc_id", "token" ), tf_count );

    Fields tf_token = new Fields( "tf_token" );
    tfPipe = new Rename( tfPipe, token, tf_token );

    // one branch counts the number of documents (D)
    Fields doc_id = new Fields( "doc_id" );
    Fields tally = new Fields( "tally" );
    Fields rhs_join = new Fields( "rhs_join" );
    Fields n_docs = new Fields( "n_docs" );
    Pipe dPipe = new Unique( "D", tokenPipe, doc_id );
    dPipe = new Each( dPipe, new Insert( tally, 1 ), Fields.ALL );
    dPipe = new Each( dPipe, new Insert( rhs_join, 1 ), Fields.ALL );
    dPipe = new SumBy( dPipe, rhs_join, tally, n_docs, long.class );

    // one branch tallies the token counts for document frequency (DF)
    Pipe dfPipe = new Unique( "DF", tokenPipe, Fields.ALL );
    Fields df_count = new Fields( "df_count" );
    dfPipe = new CountBy( dfPipe, token, df_count );

    Fields df_token = new Fields( "df_token" );
    Fields lhs_join = new Fields( "lhs_join" );
    dfPipe = new Rename( dfPipe, token, df_token );
    dfPipe = new Each( dfPipe, new Insert( lhs_join, 1 ), Fields.ALL );

    // join to bring together all the components for calculating TF-IDF
    // the D side of the join is smaller, so it goes on the RHS
    Pipe idfPipe = new HashJoin( dfPipe, lhs_join, dPipe, rhs_join );

    // the IDF side of the join is smaller, so it goes on the RHS
    Pipe tfidfPipe = new CoGroup( tfPipe, tf_token, idfPipe, df_token );

    // calculate the TF-IDF weights, per token, per document
    Fields tfidf = new Fields( "tfidf" );
    String expression = "(double) tf_count * Math.log( (double) n_docs / ( 1.0 + df_count ) )";
    ExpressionFunction tfidfExpression = new ExpressionFunction( tfidf, expression, Double.class );
    Fields tfidfArguments = new Fields( "tf_count", "df_count", "n_docs" );
    tfidfPipe = new Each( tfidfPipe, tfidfArguments, tfidfExpression, Fields.ALL );

    fieldSelector = new Fields( "tf_token", "doc_id", "tfidf" );
    tfidfPipe = new Retain( tfidfPipe, fieldSelector );
    tfidfPipe = new Rename( tfidfPipe, tf_token, token );

    // keep track of the word counts, which are useful for QA
    Pipe wcPipe = new Pipe( "wc", tfPipe );

    Fields count = new Fields( "count" );
    wcPipe = new SumBy( wcPipe, tf_token, tf_count, count, long.class );
    wcPipe = new Rename( wcPipe, tf_token, token );

    // additionally, sort by count
    wcPipe = new GroupBy( wcPipe, count, count );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .setName( "tfidf" )
     .addSource( docPipe, docTap )
     .addSource( stopPipe, stopTap )
     .addTailSink( tfidfPipe, tfidfTap )
     .addTailSink( wcPipe, wcTap );

    //run ambrose and cascading
    Flow tfidfFlow = flowConnector.connect( flowDef );
    EmbeddedAmbroseCascadingNotifier server = new EmbeddedAmbroseCascadingNotifier();
    tfidfFlow.addListener(server);
    tfidfFlow.addStepListener(server);
    tfidfFlow.complete();
    }
  }
