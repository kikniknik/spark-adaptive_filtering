package gr.auth.csd.datalab.spark.sql.execution

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, IsNotNull, NullIntolerant, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{CodegenSupport, FilterExec, SparkPlan, UnaryExecNode}

/**
  Physical plan for Filter operator. This class is a copy of `FilterExec`, except
  `doConsume` method which inserts adaptive predicate reordering, instead of
  static user predefined predicate order that is used in `FilterExec`.
 */
case class AdaptiveFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // `FilterExec` doc:
  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // `FilterExec` doc:
  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // `FilterExec` doc:
  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    // configurations
    val conf = SparkEnv.get.conf
    val verbose = conf.getBoolean("spark.sql.adaptiveFilter.verbose", false)
    val collectRate = conf.getInt("spark.sql.adaptiveFilter.collectRate", 1000)
    val calculateRate = conf.getInt("spark.sql.adaptiveFilter.calculateRate", 1000000)
    val momentum = conf.getDouble("spark.sql.adaptiveFilter.momentum", 0.3)

    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
      * FilterExec doc:
      * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
      */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) return false;
       """.stripMargin
    }

    ctx.currentVars = input

    // `FilterExec` comment:
    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generatedSeq = otherPreds.map { c =>

      // In `doConsume` every variable is declared and initialized once in whole code, as all
      // variables live in the same block of code. But in `doConsumeAdaptive` variables does
      // not exist necessarily in the same block and we don't know their order. Thus, we have to
      // declare and initialize variables in every function that they appear.
      val input_copy = input.map(_.copy())
      ctx.currentVars = input_copy

      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r) }
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // `FilterExec` comment:
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input_copy, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // `FilterExec` comment:
      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input_copy, output)}
       """.stripMargin.trim
    }

    // Adaptive metrics and variables
    // `numInputRows`: number of input rows in general. This is identical to scanOutputRows,
    //                 if scan is preceding
    // `numSeen`, `numCut`: arrays holding number of rows seen and cut for a round for each predicate
    // `cost`: array holding total evaluation times for a round for each predicate
    //
    // `ranks`: static array, that keeps ranks of predicates which are calculated based on numSeen, numCut
    //          It is updated based on calculateRate conf option.
    // `permutation`: static array, it keeps a permutation of predicates based on their
    //                 ranks at that moment
    // `ranks` and `permutation` are static, which means they are shared among all Tasks in an Executor.
    // We do it like that so that new Tasks won't have to start from zero in metrics.
    val numInputRows = ctx.freshName("numInputRows")
    val numSeen = ctx.freshName("numSeen")
    val numCut = ctx.freshName("numCut")
    val cost = ctx.freshName("cost")
    val ranks = ctx.freshName("ranks")
    val permutation = ctx.freshName("p")

    ctx.addMutableState("long", numInputRows, s"$numInputRows = 0;")
    ctx.addMutableState("long[]", numSeen,
      s"$numSeen = new long[]{${Array.fill(otherPreds.length)(1).mkString(", ")}};")
    ctx.addMutableState("long[]", numCut,
      s"$numCut = new long[]{${Array.fill(otherPreds.length)(1).mkString(", ")}};")
    ctx.addMutableState("long[]", cost,
      s"$cost = new long[]{${Array.fill(otherPreds.length)(60).mkString(", ")}};")
    ctx.addMutableState("static double[]",
      s"$ranks = new double[]{${Array.fill(otherPreds.length)(0).mkString(", ")}};", "")
    ctx.addMutableState("static int[]",
      s"$permutation = new int[]{${otherPreds.indices.mkString(", ")}};", "")

    // collect, calculate ranks rates
    val itsTimeToCalculate = s"$numInputRows % $calculateRate == ${30*collectRate}"
    val itsTimeToCollect = s"$numInputRows % $collectRate == 0"
    // `canCalculate`: a lock for preventing Tasks to conflict in ranks calculations
    val canCalculate = ctx.freshName("canCalculate")
    ctx.addMutableState("static boolean", s"$canCalculate = true;", "")


    // Ranks Calculation code:

    // Temp ranks and permutation. We need them in predicates re-sort.
    val temp_permutations = ctx.freshName("temp_p")
    val tempRanks = ctx.freshName("tempRanks")
    // `outputMetrics` and `outputRanks` are blocks of code that will output info about metrics
    // and ranks in Executors stdouts if verbose is enabled.
    val outputMetrics = if (verbose) {
      s"""
         |StringBuilder sbT = new StringBuilder();
         |sbT.append("(pred num, seen, cut, avg cost)");
         |for (int i=0; i<${otherPreds.length}; i++)
         |  sbT.append("\t(" + $permutation[i] + "," + $numSeen[$permutation[i]] + ","
         |            + $numCut[$permutation[i]] + "," + $cost[$permutation[i]] + "), ");
         |System.out.println(sbT.toString());
       """.stripMargin
    } else ""
    val outputRanks = if (verbose) {
      s"""
         |StringBuilder sb = new StringBuilder();
         |sb.append("(predicate, rate)\t\t");
         |for (int i=0; i<${otherPreds.length}; i++)
         |  sb.append("(" + $permutation[i] + "," + String.format("%.2f",$tempRanks[i]) + "), ");
         |System.out.println(sb.toString());
       """.stripMargin
    } else ""

    // ranksCalc: block of code that calculates predicate ranks and do appropriate permutation
    // scalastyle:off
    val ranksCalc =
      s"""
         |if ($itsTimeToCalculate && $canCalculate) {
         |  $canCalculate = false;
         |  ${ctx.registerComment("convert costs to average and find max average cost")}
         |  long max_cost = 0;
         |  for (int i=0; i<${otherPreds.length}; i++) {
         |    $cost[i] /= $numSeen[i];
         |    if ($cost[i] > max_cost)
         |      max_cost = $cost[i];
         |  }
         |
         |  $outputMetrics
         |
         |  ${ctx.registerComment("update ranks")}
         |  int[] $temp_permutations = new int[${otherPreds.length}];
         |  for (int i=0; i<${otherPreds.length}; i++) {
         |    $ranks[i] = $momentum*$ranks[i] + (1-$momentum)*($cost[i]*$numSeen[i] / (double) max_cost*$numCut[i]);
         |    $numSeen[i] = 1;
         |    $numCut[i] = 1;
         |    $cost[i] = 60;
         |    $temp_permutations[i] = i;
         |  }
         |
         |  ${ctx.registerComment("do permutation based on $ranks\n" +
            "insertion sort based on $ranks, that affects $temp_permutations")}
         |  double[] $tempRanks = new double[${otherPreds.length}];
         |  $tempRanks[0] = $ranks[0];
         |  for (int j, i=1; i<${otherPreds.length}; i++) {
         |    $tempRanks[i] = $ranks[i];
         |    double x = $tempRanks[i];
         |    int ix = $temp_permutations[i];
         |    for (j = i - 1; j >= 0; j--)
         |      if ($tempRanks[j] > x) {
         |        $tempRanks[j + 1] = $tempRanks[j];
         |        $temp_permutations[j + 1] = $temp_permutations[j];
         |      } else break;
         |
         |    $tempRanks[j+1] = x;
         |    $temp_permutations[j+1] = ix;
         |  }
         |
         |  $permutation = $temp_permutations;
         |
         |  $outputRanks
         |  $canCalculate = true;
         |}
       """.stripMargin
    // scalastyle:on


    // Predicates evaluation code:

    // `choosePredicate`: function that will evaluate a predicate, based on its argument.
    //                  e.g. for evaluating second predicate => choosePredicate(1)
    val choosePredName = ctx.freshName("choosePredicate")
    val choosePredCode =
      s"""
         |private final Boolean $choosePredName(int index) {
         |  switch (index) {
         |    ${generatedSeq.zipWithIndex.map { case (predicate, i) =>
                s"""
                   |case $i: {
                   |  $predicate
                   |  return true;
                   |}
               """.stripMargin
              }.mkString("\n")}
         |  }
         |  return true;
         |}
       """.stripMargin
    ctx.addNewFunction(choosePredName, choosePredCode)

    // Total block of code in processNext for filter will be a number of if statements that each
    // will call choosePredicate to evaluate all predicates in order based on current permutation.
    // But, we have 2 'modes' for this. Either we collect statistics from current row or not.
    // These 2 cases will be wrapped in an external if statement.
    val permutationsSnap = ctx.freshName("permutationsSnap")
    val predResult = ctx.freshName("predResult")
    val continue_flag = ctx.freshName("continue_flag")
    val generatedWithoutCollect =
      generatedSeq.indices.map { i =>
        s"""
           |if (!$choosePredName($permutationsSnap[$i])) continue;
           """.stripMargin
      }.mkString("\n")
    val generatedWithCollect =
      generatedSeq.indices.map { i =>
        s"""
           |$numSeen[$permutationsSnap[$i]] += 1;
           |t0 = System.nanoTime();
           |$predResult = $choosePredName($permutationsSnap[$i]);
           |$cost[$permutationsSnap[$i]] += System.nanoTime() - t0;
           |if (!$predResult) {
           |  $numCut[$permutationsSnap[$i]] += 1;
           |  $continue_flag = true;
           |//  continue;
           |}
           """.stripMargin
      }.mkString("\n")
    val generated =
      s"""
         |int[] $permutationsSnap = $permutation;
         |if ($itsTimeToCollect) {
         |  long t0;
         |  boolean $predResult;
         |  boolean $continue_flag = false;
         |  $generatedWithCollect
         |  if ($continue_flag) continue;
         |} else {
         |  $generatedWithoutCollect
         |}
       """.stripMargin


    ctx.currentVars = input

    // `FilterExec` comment:
    // As for now, we leave independent null checks as they are
    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // `FilterExec` comment:
    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    // Input for filter may come either from an InternalRow or from a batch of Rows. In the
    // first case InternalRow will be local variable and in the second case current index
    // of batch will be local variable. But, we refer to those variables from a function
    // outside of normal loop (in `prcessNext`) and we need to make these variables
    // (either of which) object fields. `prereq` contains the appropriate variable, that
    // is declared and updated at every iteration.
    val prereq = {
      val declared = ctx.mutableStates.map(_._2)
      val allpossible = input.head.code.split("\n", 3)(1).split(" = ").last.split('.')
      if (!declared.contains(allpossible(0))) {
        ctx.addMutableState("InternalRow", allpossible(0), "")
        allpossible(0)
      } else {
        val Pattern = raw"[(](\w.*)[)]".r.unanchored
        allpossible(1) match {
          case Pattern(value) =>
            ctx.addMutableState("int", value, "")
            value
          case _ => ""
        }
      }
    }

    s"""
       |this.$prereq = $prereq;
       |$ranksCalc
       |$numInputRows++;
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = FilterExec(condition, child).execute()

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

}
