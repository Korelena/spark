/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.clustering

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{Loader, MLUtils, Saveable}
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable

import scala.language.postfixOps

/**
 * Model produced by [[DependenceClustering]].
 *
 * @param t number of Markov transitions
 * @param epsi_d baseline dependence
 * @param delta_dep dependence threshold for group split
 * @param assignments an RDD of clustering [[DependenceClustering#Assignment]]s
 */
@Since("1.3.0")
class DependenceClusteringModel @Since("1.3.0") (
    @Since("1.3.0") val t: Int,
    @Since("1.3.0") val epsi_d: Float,
    @Since("1.3.0") val delta_dep: Float,
    @Since("1.3.0") val delta_e: Float,
    @Since("1.3.0") val delta_v: Float,
    @Since("1.3.0") val assignments: RDD[DependenceClustering.Assignment])
  extends Saveable with Serializable {

  @Since("1.4.0")
  override def save(sc: SparkContext, path: String): Unit = {
    DependenceClusteringModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

@Since("1.4.0")
object DependenceClusteringModel extends Loader[DependenceClusteringModel] {

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): DependenceClusteringModel = {
    DependenceClusteringModel.SaveLoadV1_0.load(sc, path)
  }

  private[clustering]
  object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.DependenceClusteringModel"

    @Since("1.4.0")
    def save(sc: SparkContext, model: DependenceClusteringModel, path: String): Unit = {
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ 
	("t" -> model.t) ~ ("t" -> model.t)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val dataRDD = model.assignments.toDF()
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    @Since("1.4.0")
    def load(sc: SparkContext, path: String): DependenceClusteringModel = {
      implicit val formats = DefaultFormats
      val sqlContext = SQLContext.getOrCreate(sc)

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val t = (metadata \ "t").extract[Int]
      val epsi_d = (metadata \ "epsi_d").extract[Float]
      val delta_dep = (metadata \ "delta_dep").extract[Float]
      val delta_e = (metadata \ "delta_e").extract[Float]
      val delta_v = (metadata \ "delta_v").extract[Float]
      val assignments = sqlContext.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[DependenceClustering.Assignment](assignments.schema)

      val assignmentsRDD = assignments.map {
        case Row(id: Long, cluster: Int, eval: Float) => 
	      DependenceClustering.Assignment(id, cluster)
      }

      new DependenceClusteringModel(t, epsi_d, delta_dep, delta_e, delta_v, assignmentsRDD)
    }
  }
}

/**
 * Dependence Clustering (DEP), a graph clustering algorithm developed by
 * [[http://www.sciencedirect.com/science/article/pii/S095070511400015X Park and Lee]].
 *
 * @param t Number of diffusion iterations.
 * @param maxIterations Maximum number of iterations of the DEP algorithm.
 * @param initMode Initialization mode.
 *
 * @see [[http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)]]
 */
@Since("1.3.0")
class DependenceClustering private[clustering] (
    private var t: Int,
    private var epsi_d: Float,
    private var delta_dep: Float,
    private var delta_e: Float,
    private var delta_v: Float,
    private var maxIterations: Int,
    private var initMode: String) extends Serializable {

  import org.apache.spark.mllib.clustering.DependenceClustering._

  /**
   * Constructs a DEP instance with default parameters: 
   * {t: 2, epsi_d: 0.0, delta_dep: 0.0, delta_e = 0.0, delta_v = 0.0, maxIterations: 100,
   * initMode: "random"}.
   */
  @Since("1.3.0")
  def this() = this(t = 2, epsi_d = 0.0f, delta_dep = 0.0f, delta_e = 0.0f, 
		    delta_v = 0.0f, maxIterations = 100, initMode = "random")

  /**
   * Set the number of clusters.
   */
  @Since("1.3.0")
  def setT(t: Int): this.type = {
    this.t = t
    this
  }

  /**
   * Set epsi_d.
   */
  @Since("1.3.0")
  def setEpsiD(epsi_d: Float): this.type = {
    this.epsi_d = epsi_d
    this
  }

  /**
   * Set delta_dep.
   */
  @Since("1.3.0")
  def setDeltaDep(delta_dep: Float): this.type = {
    this.delta_dep = delta_dep
    this
  }

  /**
   * Set delta_e.
   */
  @Since("1.3.0")
  def setDeltaE(delta_e: Float): this.type = {
    this.delta_e = delta_e
    this
  }

  /**
   * Set delta_v.
   */
  @Since("1.3.0")
  def setDeltaV(delta_v: Float): this.type = {
    this.delta_v = delta_v
    this
  }

  /**
   * Set maximum number of iterations of the power iteration loop
   */
  @Since("1.3.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization mode. This can be either "random" to use a random vector
   * as vertex properties, or "degree" to use normalized sum similarities. Default: random.
   */
  @Since("1.3.0")
  def setInitializationMode(mode: String): this.type = {
    this.initMode = mode match {
      case "random" | "degree" => mode
      case _ => throw new IllegalArgumentException("Invalid initialization mode: " + mode)
    }
    this
  }


  /**
   * Run the DEP algorithm.
   *
   * @param similarities an RDD of (i, j, s,,ij,,) tuples representing the affinity matrix, which is
   *                     the matrix A in the PIC paper. The similarity s,,ij,, must be nonnegative.
   *                     This is a symmetric matrix and hence s,,ij,, = s,,ji,,. For any (i, j) with
   *                     nonzero similarity, there should be either (i, j, s,,ij,,) or
   *                     (j, i, s,,ji,,) in the input. Tuples with i = j are ignored, because we
   *                     assume s,,ij,, = 0.0.
   *
   * @return a [[DependenceClusteringModel]] that contains the clustering result
   */
  @Since("1.3.0")
  def run(similarities: RDD[(Long, Long, Double)]): DependenceClusteringModel = {
    val w = normalize(similarities, epsi_d, t, delta_e, delta_v)
    val w0 = initMode match {
      case "random" => randomInit(w)
      case "degree" => initDegreeVector(w)
    }

    val ne : Long = w0.numVertices
    var continueIteration = true

    /* Storage for a group info */
    case class DebugInfo(var gd_full: Double, /* group dependence for index_full slice */
                                var gd_m: Double, /* group dependence for index_m slice */
                                var gd_p: Double, /* group dependence for index_p slice */
                                var index_m: Set[Long], /* slice of items from index_full that 
				received negative dependence score */
                                var index_p: Set[Long],  /* slice of items from index_full that 
				received positive dependence score */
                                var index_full: Set[Long],  /* slice of items from the entire item pool */
                                var tried: Boolean, /* flag for whether split of index_full into 
				index_m and index_p was tried */
                                var split: Boolean)  /* flag for whether split of index_full into index_m 
				and index_p was successful */

   /* Storage of info for all groups */
   val debug = mutable.Map(0 -> DebugInfo(w0.edges.map(e => e.attr).reduce(_ + _),
                                0.0,
                                0.0,
                                Set(),
                                Set(),
                                0L until ne toSet,
                                false,
                                false))

   val maxDep = w0.edges.map(e => e.attr)
                        .filter(e => e > 0.0)
                        .reduce(_ + _)

   while(continueIteration == true) {
        continueIteration = false
        var maxDepGain = 0.0
        var maxGroup = -1

        debug.keys.foreach{ i =>
                           if (!debug(i).tried) {
                                debug(i).tried = true
                                val selected_indices = debug(i).index_full
				   
                                if (selected_indices.size > 1) {
                                var subM = w0.subgraph(vpred = (id, d) => selected_indices contains id)
                                var md = dep(subM)

                                debug(i).index_m = md.assignments.filter(a => a.cluster == 0)
					                         .map(a => a.id)
					                         .collect().toSet
                                debug(i).index_p = md.assignments.filter(a => a.cluster == 1)
					                         .map(a => a.id)
					                         .collect().toSet

                                debug(i).gd_m = if (debug(i).index_m.size > 0) subM.subgraph(
                                        vpred = (id, d) => debug(i).index_m contains id)
                                        .edges.map(e => e.attr)
                                        .fold(0.0)(_ + _) else 0.0
                                debug(i).gd_p = if (debug(i).index_p.size > 0) subM.subgraph(
                                        vpred = (id, d) => debug(i).index_p contains id)
                                        .edges.map(e => e.attr)
                                        .fold(0.0)(_ + _) else 0.0

                                if (!debug(i).index_m.isEmpty && !debug(i).index_p.isEmpty) {
                                    debug(i).split = true
                                }

                                print( "Key = " + i )
                                // scalastyle:off println
                                println(" Value = " + debug(i))
                                // scalastyle:on println
                                }
                          }

                          if (debug(i).split) {
                                var depGain = debug(i).gd_m + debug(i).gd_p - debug(i).gd_full
                                if (depGain > maxDepGain + 1E-16) {
                                        maxDepGain = depGain
                                        maxGroup = i
                              }
                          }
       }
       if (maxDepGain > maxDep*delta_dep) {
                continueIteration = true
                var newGroup = debug.size
		/* Create and initialize info container for the new newGroup group */
                debug += (newGroup -> DebugInfo(debug(maxGroup).gd_m,
                                        0.0,
                                        0.0,
                                        Set(),
                                        Set(),
                                        debug(maxGroup).index_m,
                                        false,
                                        false))
                /* Update info for the old maxGroup group */
                debug(maxGroup).gd_full = debug(maxGroup).gd_p
                debug(maxGroup).index_full = debug(maxGroup).index_p
                debug(maxGroup).gd_m = 0.0
                debug(maxGroup).gd_p = 0.0
                debug(maxGroup).index_m = Set()
                debug(maxGroup).index_p = Set()
                debug(maxGroup).tried = false
                debug(maxGroup).split = false
       }
   }

   val assignments = debug.keys.flatMap(i => debug(i).index_full zip Stream.continually(i))
                               .map{case (id, cl) => Assignment(id, cl)}
   new DependenceClusteringModel(t, epsi_d, delta_dep, delta_e, 
                                 delta_v, similarities.context.parallelize(assignments.toSeq))
  }

  /**
   * A Java-friendly version of [[DependenceClustering.run]].
   */
  @Since("1.3.0")
  def run(similarities: JavaRDD[(java.lang.Long, java.lang.Long, java.lang.Double)])
    : DependenceClusteringModel = {
    run(similarities.rdd.asInstanceOf[RDD[(Long, Long, Double)]])
  }

  /**
   * Runs the DEP algorithm.
   *
   * @param w The normalized affinity matrix, which is the matrix W in the DEP paper with
   *          w,,ij,, = a,,ij,, / d,,ii,, as its edge properties and the initial vector of the power
   *          iteration as its vertex properties.
   */
  private def dep(w: Graph[Double, Double]): DependenceClusteringModel = {
    val v = powerIter(w, maxIterations)
    val assignments = v.mapPartitions({ iter =>
      iter.map { case (id, eval) =>
        Assignment(id, if (eval > 0) 1 else 0)
      }
    }, preservesPartitioning = true)
    new DependenceClusteringModel(t, epsi_d, delta_dep, delta_e, delta_v, assignments)
  }
}

@Since("1.3.0")
object DependenceClustering extends Logging {

  /**
   * Cluster assignment.
   * @param id node id
   * @param cluster assigned cluster id
   */
  @Since("1.3.0")
  case class Assignment(id: Long, cluster: Int)

  /**
   * Normalizes the affinity matrix (A) by row sums and returns the normalized affinity matrix (W).
   */
  private[clustering]
  def normalize(similarities: RDD[(Long, Long, Double)], epsi_d: Float, 
                t: Int, delta_e: Float, delta_v: Float)
    : Graph[Double, Double] = {
    val edges = similarities.flatMap { case (i, j, s) =>
      if (s < 0.0) {
        throw new SparkException("Similarity must be nonnegative but found s($i, $j) = $s.")
      }
      if (i != j) {
        Seq(Edge(i, j, s), Edge(j, i, s))
      } else {
        None
      }
    }
    var gA = Graph.fromEdges(edges, 0.0)
    val ne = gA.numVertices

    // Propagate diffusion
    for (a <- 1 to t) {
        var outDegrees: VertexRDD[Int] = gA.outDegrees

        // Sum all in-edges
        var vD_ = gA.aggregateMessages[Double](
        sendMsg = ctx => {
                ctx.sendToDst(ctx.attr)
        },
        mergeMsg = _ + _,
        TripletFields.EdgeOnly)

        var gB_ = Graph(vD_.join(outDegrees), gA.edges)

        var vD__ = gB_.aggregateMessages[mutable.Map[Long, Double]](
                sendMsg = ctx => {
                if (ctx.attr * ctx.srcAttr._1  > delta_v) {
                        ctx.sendToSrc(mutable.Map(ctx.dstId -> ctx.attr))
                } else {
                        ctx.sendToSrc(mutable.Map[Long, Double]())
                }
                },
                mergeMsg = {(a, b) => a ++= b},
                TripletFields.All)

        var gB__ = Graph(vD__, gA.edges)
        var vD___ = gB__.aggregateMessages[Set[(Long, Double)]](
                sendMsg = ctx => {
                var newEdges : Set[(Long, Double)] = Set()
                if (ctx.srcAttr != null && ctx.dstAttr != null) {
                ctx.dstAttr.keys.foreach{ i =>
                        if (!ctx.srcAttr.contains(i) && ctx.attr > delta_e) {
                                newEdges = newEdges union Set((i, ctx.attr * ctx.dstAttr(i)))
                        }
                }
                }
                ctx.sendToSrc(newEdges)
                },
                mergeMsg = {(a, b) => a union b},
                TripletFields.All)

        var edgesAdd = vD___.flatMap{case (id, s) => s.map(e => Edge(id, e._1, e._2))}

        var gAdd = Graph.fromEdges(edgesAdd, 0.0)

        var gDiff: Graph[Double, Double] = Graph(
                gA.vertices,
                gA.edges.union(gAdd.edges)
        ).groupEdges( (attr1, attr2) => if (attr1 > attr2) attr1 else attr2 )

        gA = gDiff.mapVertices((id, attr) => 0.0)
    }



    // Normalize rows
    val vD = gA.aggregateMessages[Double](
      sendMsg = ctx => {
        ctx.sendToSrc(ctx.attr)
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly)

    val gB = Graph(vD, gA.edges)
      .mapTriplets(
        e => e.attr / math.max(e.srcAttr, MLUtils.EPSILON),
        new TripletFields(/* useSrc */ true,
                          /* useDst */ false,
                          /* useEdge */ true))
    // Normalize columns
    val gC = Graph.fromEdges(gB.edges, 0.0)
    val vE = gC.aggregateMessages[Double](
      sendMsg = ctx => {
        ctx.sendToDst(ctx.attr)
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly)

    Graph(vE, gC.edges)
      .mapTriplets(
        e => e.attr / math.max(e.dstAttr / ne.toDouble, MLUtils.EPSILON),
        new TripletFields(/* useSrc */ false,
                          /* useDst */ true,
                          /* useEdge */ true))
      .mapEdges(e => e.attr.toDouble - 1.0 - epsi_d.toDouble)
  }

  /**
   * Generates random vertex properties (v0) to start power iteration.
   *
   * @param g a graph representing the normalized affinity matrix (W)
   * @return a graph with edges representing W and vertices representing a random vector
   *         with unit 1-norm
   */
  private[clustering]
  def randomInit(g: Graph[Double, Double]): Graph[Double, Double] = {
    val r = g.vertices.mapPartitionsWithIndex(
      (part, iter) => {
        val random = new XORShiftRandom(part)
        iter.map { case (id, _) =>
          (id, random.nextGaussian())
        }
      }, preservesPartitioning = true).cache()
    val sum = r.values.map(math.abs).sum()
    val v0 = r.mapValues(x => x / sum)
    Graph(VertexRDD(v0), g.edges)
  }

  /**
   * Generates the degree vector as the vertex properties (v0) to start power iteration.
   *
   * @param g a graph representing the normalized affinity matrix (W)
   * @return a graph with edges representing W and vertices representing the degree vector
   */
  private[clustering]
  def initDegreeVector(g: Graph[Double, Double]): Graph[Double, Double] = {
    val sum = g.vertices.values.sum()
    val v0 = g.vertices.mapValues(_ / sum)
    Graph(VertexRDD(v0), g.edges)
  }

  /**
   * Runs power iteration.
   * @param g input graph with edges representing the normalized affinity matrix (W) and vertices
   *          representing the initial vector of the power iterations.
   * @param maxIterations maximum number of iterations
   * @return a [[VertexRDD]] representing the pseudo-eigenvector
   */
  private[clustering]
  def powerIter(
      g: Graph[Double, Double],
      maxIterations: Int): VertexRDD[Double] = {
    // the default tolerance used in the PIC paper, with a lower bound 1e-8
    val tol = math.max(1e-5 / g.vertices.count(), 1e-8)
    var prevDelta = Double.MaxValue
    var diffDelta = Double.MaxValue
    var curG = g
    for (iter <- 0 until maxIterations if math.abs(diffDelta) > tol) {
      val msgPrefix = s"Iteration $iter"
      // multiply W by vt
      val v = curG.aggregateMessages[Double](
        sendMsg = ctx => ctx.sendToSrc(ctx.attr * ctx.dstAttr),
        mergeMsg = _ + _,
        new TripletFields(/* useSrc */ false,
                          /* useDst */ true,
                          /* useEdge */ true)).cache()
      // normalize v
      val norm = v.values.map(math.abs).sum()
      logInfo(s"$msgPrefix: norm(v) = $norm.")
      val v1 = v.mapValues(x => x / norm)
      // compare difference
      val delta = curG.joinVertices(v1) { case (_, x, y) =>
        math.abs(x - y)
      }.vertices.values.sum()
      logInfo(s"$msgPrefix: delta = $delta.")
      diffDelta = math.abs(delta - prevDelta)
      logInfo(s"$msgPrefix: diff(delta) = $diffDelta.")
      // update v
      curG = Graph(VertexRDD(v1), g.edges)
      prevDelta = delta
    }
    curG.vertices
  }

}
