package gr.auth.csd.datalab.spark.sql

import gr.auth.csd.datalab.spark.sql.execution.AdaptiveFilterStrategy
import org.apache.spark.sql.SparkSessionExtensions

class AdaptiveFilterExtensionInjector extends ((SparkSessionExtensions) => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      AdaptiveFilterStrategy(session.sessionState.conf)
    }
  }
}
