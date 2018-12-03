package gr.auth.csd.datalab.spark.sql.execution

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.internal.SQLConf

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

  def convertFilterExecToAdaptive(sparkPlan: SparkPlan): SparkPlan = sparkPlan match {
    case execution.ProjectExec(condition, child) =>
      execution.ProjectExec(condition, convertFilterExecToAdaptive(child))
    case execution.FilterExec(condition, child) =>
      gr.auth.csd.datalab.spark.sql.execution.AdaptiveFilterExec(condition, convertFilterExecToAdaptive(child))
    case other => other
  }
}
