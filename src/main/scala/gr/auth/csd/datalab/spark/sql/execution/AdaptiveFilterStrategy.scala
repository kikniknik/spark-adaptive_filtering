package gr.auth.csd.datalab.spark.sql.execution

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.internal.SQLConf

/**
  * Catalyst strategy that converts logical Filter to physical AdaptiveFilterExec, instead of FilterExec.
  * It is meant to be inserted in Spark planner as an external extra Strategy.
  */
case class AdaptiveFilterStrategy(conf: SQLConf) extends SparkStrategy{
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val adaptiveFilterEnabled = SparkEnv.get.conf.getBoolean("spark.sql.adaptiveFilter.enabled", true)
    if (adaptiveFilterEnabled) {
      plan match {
        case logical.Filter(condition, child) =>
          gr.auth.csd.datalab.spark.sql
            .execution.AdaptiveFilterExec(condition, planLater(child)) :: Nil
        case f: logical.TypedFilter =>
          gr.auth.csd.datalab.spark.sql
            .execution.AdaptiveFilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
        case PhysicalOperation(_) =>
          // There are some Strategies (eg `FileSourceStrategy`, `InMemoryScans`, etc) that can see a
          // parental to a filter physical operator (like a Project) in the plan tree and they convert
          // logical Projects to physical `ProjectExec` and logical Filters to physical `FilterExec`.
          // We run them here in advance and later we convert `FilterExec` to `AdaptiveFilterExec` with
          // `convertFilterExecToAdaptive` method.
          val fsStrategies = FileSourceStrategy(plan).map(convertFilterExecToAdaptive)
          if (fsStrategies.isEmpty) {
            DataSourceStrategy(conf)(plan).map(convertFilterExecToAdaptive)
          } else {
            fsStrategies
          }
        case _ => Nil
      }
    } else {
      Nil
    }
  }

  /**
    * It converts all `FilterExec` operators of a `SparkPlan` to `AdaptiveFilterExec`.
    */
  def convertFilterExecToAdaptive(sparkPlan: SparkPlan): SparkPlan = sparkPlan match {
    case execution.ProjectExec(condition, child) =>
      execution.ProjectExec(condition, convertFilterExecToAdaptive(child))
    case execution.FilterExec(condition, child) =>
      gr.auth.csd.datalab.spark.sql.execution.AdaptiveFilterExec(condition, convertFilterExecToAdaptive(child))
    case other => other
  }
}
