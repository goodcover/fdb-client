package com.goodcover.fdb.es.pekko

import com.goodcover.fdb.FoundationDbConfig
import com.typesafe.config.Config
import org.apache.pekko.actor.{ ActorSystem, ExtendedActorSystem }

import scala.jdk.CollectionConverters.CollectionHasAsScala

case class FdbPekkoJournalConfig(
  config: FdbPekkoConfigProvider,
  clusterConfig: FoundationDbConfig,
)

object FdbPekkoJournalConfig {
  def apply(system: ActorSystem, config: Config, path: Option[String]): FdbPekkoJournalConfig = {
    // Journal version
    val subConfig   = path.fold(config)(config.getConfig)
    val classHelper = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val theConfig   = classHelper
      .createInstanceFor[FdbPekkoConfigProvider](subConfig.getString("configProvider"), Seq(classOf[Config] -> config))
      .get

    val fdbConfigGetter = subConfig.entrySet().asScala
    val stringMap       = fdbConfigGetter
      .map(pair => pair.getKey -> pair.getValue.unwrapped().toString)
      .toMap

    val fdbConfig = FoundationDbConfig.fromMap(stringMap)

    FdbPekkoJournalConfig(theConfig, fdbConfig)
  }

}
