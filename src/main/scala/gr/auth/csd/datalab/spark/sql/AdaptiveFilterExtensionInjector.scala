package gr.auth.csd.datalab.spark.sql

import gr.auth.csd.datalab.spark.sql.execution.AdaptiveFilterStrategy
import org.apache.spark.sql.SparkSessionExtensions

/**
  * It's purpose is to inject `AdaptiveFilterStrategy` in `SparkPlanner`'s extra strategies.
  * It's meant to be used with `spark.sql.extensions` config parameter.
  */
class AdaptiveFilterExtensionInjector extends ((SparkSessionExtensions) => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      AdaptiveFilterStrategy(session.sessionState.conf)
    }
  }
}
