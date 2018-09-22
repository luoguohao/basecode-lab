package scalikejdbc.mapper

import java.io.{File, FileNotFoundException}
import java.util.Locale.{ENGLISH => en}
import java.util.Properties

import scala.util.control.Exception.{ignoring, ultimately}

object ScalikejdbcCodeGenerator {

  object autoImport {

    case class JDBCSettings(driver: String, url: String, username: String, password: String, schema: String)

    case class GeneratorSettings(
                                  packageName: String,
                                  template: String,
                                  testTemplate: String,
                                  lineBreak: String,
                                  @deprecated("will be removed", "3.3.0") caseClassOnly: Boolean,
                                  encoding: String,
                                  autoConstruct: Boolean,
                                  defaultAutoSession: Boolean,
                                  dateTimeClass: DateTimeClass,
                                  tableNameToClassName: String => String,
                                  columnNameToFieldName: String => String,
                                  returnCollectionType: ReturnCollectionType,
                                  view: Boolean,
                                  tableNamesToSkip: collection.Seq[String],
                                  baseTypes: collection.Seq[String],
                                  companionBaseTypes: collection.Seq[String],
                                  tableNameToSyntaxName: String => String,
                                  tableNameToSyntaxVariableName: String => String)

  }

  import autoImport._

  private[this] def getString(props: Properties, key: String): Option[String] =
    Option(props.get(key)).map { value =>
      val str = value.toString
      if (str.startsWith("\"") && str.endsWith("\"") && str.length >= 2) {
        str.substring(1, str.length - 1)
      } else str
    }

  private[this] def commaSeparated(props: Properties, key: String): collection.Seq[String] =
    getString(props, key).map(_.split(',').map(_.trim).filter(_.nonEmpty).toList).getOrElse(Nil)

  private[this] final val JDBC = "jdbc."
  private[this] final val JDBC_DRIVER = JDBC + "driver"
  private[this] final val JDBC_URL = JDBC + "url"
  private[this] final val JDBC_USER_NAME = JDBC + "username"
  private[this] final val JDBC_PASSWORD = JDBC + "password"
  private[this] final val JDBC_SCHEMA = JDBC + "schema"

  private[this] final val GENERATOR = "generator."
  private[this] final val PACKAGE_NAME = GENERATOR + "packageName"
  private[this] final val TEMPLATE = GENERATOR + "template"
  private[this] final val TEST_TEMPLATE = GENERATOR + "testTemplate"
  private[this] final val LINE_BREAK = GENERATOR + "lineBreak"
  private[this] final val ENCODING = GENERATOR + "encoding"
  private[this] final val AUTO_CONSTRUCT = GENERATOR + "autoConstruct"
  private[this] final val DEFAULT_AUTO_SESSION = GENERATOR + "defaultAutoSession"
  private[this] final val DATETIME_CLASS = GENERATOR + "dateTimeClass"
  private[this] final val RETURN_COLLECTION_TYPE = GENERATOR + "returnCollectionType"
  private[this] final val VIEW = GENERATOR + "view"
  private[this] final val TABLE_NAMES_TO_SKIP = GENERATOR + "tableNamesToSkip"
  private[this] final val BASE_TYPES = GENERATOR + "baseTypes"
  private[this] final val COMPANION_BASE_TYPES = GENERATOR + "companionBaseTypes"

  private[this] val jdbcKeys = Set(
    JDBC_DRIVER, JDBC_URL, JDBC_USER_NAME, JDBC_PASSWORD, JDBC_SCHEMA)
  private[this] val generatorKeys = Set(
    PACKAGE_NAME, TEMPLATE, TEST_TEMPLATE, LINE_BREAK,
    ENCODING, AUTO_CONSTRUCT, DEFAULT_AUTO_SESSION, DATETIME_CLASS, RETURN_COLLECTION_TYPE,
    VIEW, TABLE_NAMES_TO_SKIP, BASE_TYPES, COMPANION_BASE_TYPES)
  private[this] val allKeys = jdbcKeys ++ generatorKeys


  private[this] def loadJDBCSettings(props: Properties): JDBCSettings = {
    JDBCSettings(
      driver = getString(props, JDBC_DRIVER).getOrElse(throw new IllegalStateException(s"Add $JDBC_DRIVER to project/scalikejdbc-mapper-generator.properties")),
      url = getString(props, JDBC_URL).getOrElse(throw new IllegalStateException(s"Add $JDBC_URL to project/scalikejdbc-mapper-generator.properties")),
      username = getString(props, JDBC_USER_NAME).getOrElse(""),
      password = getString(props, JDBC_PASSWORD).getOrElse(""),
      schema = getString(props, JDBC_SCHEMA).orNull[String])
  }

  private[this] val loadGeneratorSettings: Properties => GeneratorSettings = {
    props =>
      val defaultConfig = GeneratorConfig()
      GeneratorSettings(
        packageName = getString(props, PACKAGE_NAME).getOrElse(defaultConfig.packageName),
        template = getString(props, TEMPLATE).getOrElse(defaultConfig.template.name),
        testTemplate = getString(props, TEST_TEMPLATE).getOrElse(GeneratorTestTemplate.specs2unit.name),
        lineBreak = getString(props, LINE_BREAK).getOrElse(defaultConfig.lineBreak.name),
        caseClassOnly = defaultConfig.caseClassOnly,
        encoding = getString(props, ENCODING).getOrElse(defaultConfig.encoding),
        autoConstruct = getString(props, AUTO_CONSTRUCT).map(_.toBoolean).getOrElse(defaultConfig.autoConstruct),
        defaultAutoSession = getString(props, DEFAULT_AUTO_SESSION).map(_.toBoolean).getOrElse(defaultConfig.defaultAutoSession),
        dateTimeClass = getString(props, DATETIME_CLASS).map {
          name => DateTimeClass.map.getOrElse(name, sys.error("does not support " + name))
        }.getOrElse(defaultConfig.dateTimeClass),
        defaultConfig.tableNameToClassName,
        defaultConfig.columnNameToFieldName,
        returnCollectionType = getString(props, RETURN_COLLECTION_TYPE).map { name =>
          val CBF = "canbuildfrom"
          name.toLowerCase(en) match {
            case CBF =>
              ReturnCollectionType.Factory
            case n =>
              ReturnCollectionType.map.getOrElse(
                n,
                sys.error(s"does not support $name. support types are ${ReturnCollectionType.map.keys.mkString(", ")}"))
          }
        }.getOrElse(defaultConfig.returnCollectionType),
        view = getString(props, VIEW).map(_.toBoolean).getOrElse(defaultConfig.view),
        tableNamesToSkip = getString(props, TABLE_NAMES_TO_SKIP).map(_.split(",").toList).getOrElse(defaultConfig.tableNamesToSkip),
        baseTypes = commaSeparated(props, BASE_TYPES),
        companionBaseTypes = commaSeparated(props, COMPANION_BASE_TYPES),
        tableNameToSyntaxName = defaultConfig.tableNameToSyntaxName,
        tableNameToSyntaxVariableName = defaultConfig.tableNameToSyntaxVariableName)
  }

  private[this] def loadPropertiesFromFile(): Either[FileNotFoundException, Properties] = {
    val props = new java.util.Properties
    try {
      using(new java.io.FileInputStream("project/scalikejdbc-mapper-generator.properties")) {
        inputStream => props.load(inputStream)
      }
    } catch {
      case e: FileNotFoundException =>
    }
    if (props.isEmpty) {
      try {
        using(new java.io.FileInputStream("project/scalikejdbc.properties")) {
          inputStream => props.load(inputStream)
        }
        Right(props)
      } catch {
        case e: FileNotFoundException =>
          Left(e)
      }
    } else {
      Right(props)
    }
  }

  def generatorConfig(srcDir: File, testDir: File, generatorSettings: GeneratorSettings) =
    GeneratorConfig(
      srcDir = srcDir.getAbsolutePath,
      testDir = testDir.getAbsolutePath,
      packageName = generatorSettings.packageName,
      template = GeneratorTemplate(generatorSettings.template),
      testTemplate = GeneratorTestTemplate(generatorSettings.testTemplate),
      lineBreak = LineBreak(generatorSettings.lineBreak),
      caseClassOnly = generatorSettings.caseClassOnly,
      encoding = generatorSettings.encoding,
      autoConstruct = generatorSettings.autoConstruct,
      defaultAutoSession = generatorSettings.defaultAutoSession,
      dateTimeClass = generatorSettings.dateTimeClass,
      tableNameToClassName = generatorSettings.tableNameToClassName,
      columnNameToFieldName = generatorSettings.columnNameToFieldName,
      returnCollectionType = generatorSettings.returnCollectionType,
      view = generatorSettings.view,
      tableNamesToSkip = generatorSettings.tableNamesToSkip,
      tableNameToBaseTypes = _ => generatorSettings.baseTypes,
      tableNameToCompanionBaseTypes = _ => generatorSettings.companionBaseTypes,
      tableNameToSyntaxName = generatorSettings.tableNameToSyntaxName,
      tableNameToSyntaxVariableName = generatorSettings.tableNameToSyntaxVariableName)


  private def generator(tableNames: Seq[String], className: Option[String], srcDir: File, testDir: File, jdbc: JDBCSettings, generatorSettings: GeneratorSettings): Seq[Option[CodeGenerator]] = {
    val config = generatorConfig(srcDir, testDir, generatorSettings)
    Class.forName(jdbc.driver) // load specified jdbc driver
    val model = Model(jdbc.url, jdbc.username, jdbc.password)
    tableNames.map { tableName =>
      model.table(jdbc.schema, tableName)
        .orElse(model.table(jdbc.schema, tableName.toUpperCase(en)))
        .orElse(model.table(jdbc.schema, tableName.toLowerCase(en)))
        .map { table =>
          Option(new CodeGenerator(table, className)(config))
        } getOrElse {
        println("The table is not found.")
        None
      }
    }
  }

  def allGenerators(srcDir: File, testDir: File, jdbc: JDBCSettings, generatorSettings: GeneratorSettings): collection.Seq[CodeGenerator] = {
    val config = generatorConfig(srcDir, testDir, generatorSettings)
    val className = None
    Class.forName(jdbc.driver) // load specified jdbc driver
    val model = Model(jdbc.url, jdbc.username, jdbc.password)
    val tableAndViews = if (generatorSettings.view) {
      model.allTables(jdbc.schema) ++ model.allViews(jdbc.schema)
    } else {
      model.allTables(jdbc.schema)
    }

    tableAndViews.map { table =>
      new CodeGenerator(table, className)(config)
    }
  }

  def generatorWithTable(tableNames: Seq[String], srcDir: File, testDir: File, properties: Properties): Unit = {

    generator(tableNames, None, srcDir, testDir, loadJDBCSettings(properties), loadGeneratorSettings(properties)) foreach { gen =>
      gen foreach { x =>
        x.writeModel()
        x.writeSpec(x.specAll())
      }
    }
  }

  def using[R <: {def close() : Unit}, A](resource: R)(f: R => A): A = ultimately {
    ignoring(classOf[Throwable]) apply resource.close()
  } apply f(resource)
}
