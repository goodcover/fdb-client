package org.apache.spark.sql.fdb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{
  CatalogPlugin,
  Identifier,
  NamespaceChange,
  SupportsNamespaces,
  Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters.*

class FdbCatalog extends CatalogPlugin with TableCatalog with SupportsNamespaces {

  private var config: FdbCatalog.FdbCatalogConfig                        = _
  private var catalogName: String                                        = _
  private var initializeOptions: CaseInsensitiveStringMap                = _
  private var tableConfigs: Map[String, FdbTable.MetadataWith[ReadConf]] = Map.empty
  private var helpers: Map[String, StorageHelper]                        = Map.empty

  private def getNsIdentifier(namespace: Array[String]) =
    if (namespace.length == 0 || namespace.length == 1) {
      namespace.mkString("")
    } else throw new NoSuchNamespaceException(namespace)

  private def getNs(namespace: Array[String]): (FdbTable.MetadataWith[ReadConf], StorageHelper) = {
    val ns = getNsIdentifier(namespace)

    (
      tableConfigs.getOrElse(ns, throw new NoSuchNamespaceException(namespace)),
      helpers.getOrElse(ns, throw new NoSuchNamespaceException(namespace))
    )
  }

  private def toNs(namespace: String): Array[String] = Array(namespace)

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    initializeOptions = options
    config = FdbCatalog.fromOptions(options)
    catalogName = name
    config.ns.foreach { case (namespace, tableConfig) =>
      val helper = StorageHelper(config.dbConfig)
      helpers = helpers.updated(namespace, helper)

      val metaData = helper.metaData(tableConfig) match {
        case Some(value) => value
        case None        =>
          throw new IllegalStateException(
            s"Missing metadata at the location ${tableConfig.metaKeySpacePath}, you'll need to manually initialize it outside of spark"
          )
      }

      tableConfigs = tableConfigs.updated(namespace, FdbTable.MetadataWith(tableConfig, config.dbConfig, metaData))
    }
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val (tc, _) = getNs(namespace)
    val mt      = tc.recordMetaData.getRecordsDescriptor.getMessageTypes.asScala
    mt.map(mt => Identifier.of(namespace, mt.getName)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val (tc, _)          = getNs(ident.namespace())
    val localTableConfig = tc.config.copy(recordType = ident.name())
    FdbTable(
      SparkSession.active,
      s"FdbCatalog:${name()}:${getNsIdentifier(ident.namespace())}:${ident.name()}",
      initializeOptions,
      FdbTable.MetadataWith(localTableConfig, config.dbConfig, tc.recordMetaData)
    )
  }

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): Table = throw new UnsupportedOperationException(
    "FdbCatalog does not allow creating of tables through the spark interface"
  )

  override def alterTable(ident: Identifier, changes: TableChange*): Table = throw new UnsupportedOperationException(
    "FdbCatalog does not allow altering of tables through the spark interface"
  )

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException(
    "FdbCatalog does not allow dropping of tables through the spark interface"
  )

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new UnsupportedOperationException(
    "FdbCatalog does not allow renaming of tables through the spark interface"
  )

  override def listNamespaces(): Array[Array[String]] = tableConfigs.keys.toArray.map(key => toNs(key))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    tableConfigs.keys.toArray.filter(_ == getNsIdentifier(namespace)).map(toNs)

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    val (tc, _) = getNs(namespace)
    tc.config.options.asJava
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException(
      "FdbCatalog does not allow creating of namespaces through the spark interface, since you pass in the namespaces via the configuration option, this isn't an issue."
    )

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException(
      "FdbCatalog does not allow renaming of tables through the spark interface"
    )

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = throw new UnsupportedOperationException(
    "FdbCatalog does not allow renaming of tables through the spark interface"
  )
}

object FdbCatalog {
  val CATALOG: String = classOf[FdbCatalog].getName

  case class FdbCatalogConfig(dbConfig: SparkFdbConfig, ns: Map[String, ReadConf])

  private val prefix = "namespace"

  def fromOptions(options: CaseInsensitiveStringMap): FdbCatalogConfig = {
    val totalOptions = options.entrySet().asScala.map(es => es.getKey -> es.getValue)

    val fdbConfig = SparkFdbConfig.fromMap(options.asScala.toMap)

    val namespaceTriples = totalOptions.collect {
      case (k, v) if k.startsWith(s"$prefix.") =>
        // guaranteed to work
        val splitKey  = k.split("\\.")
        val namespace = splitKey(1)
        val restKey   = splitKey.drop(2).mkString(".")

        (namespace, restKey, v)
    }.groupBy(_._1).map { case (ns, setOfTriples) =>
      ns -> setOfTriples.toVector.map { case (_, k, v) =>
        k -> v
      }.toMap
    }

    val tableConfigs = namespaceTriples.map { case (k, v) =>
      k -> ReadConf.fromOptions(new CaseInsensitiveStringMap(v.asJava))
    }

    FdbCatalogConfig(fdbConfig, tableConfigs)
  }
}
