/**
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

package kafka.api

import java.io.File
import java.util.Properties
import javax.security.auth.login.Configuration

import kafka.security.minikdc.MiniKdc
import kafka.server.KafkaConfig
import kafka.utils.JaasTestUtils.{JaasSection, Krb5LoginModule, ZkDigestModule}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.config.SaslConfigs

/*
 * Implements an enumeration for the modes enabled here:
 * zk only, kafka only, both, custom KafkaServer.
 */
sealed trait SaslSetupMode
case object ZkSasl extends SaslSetupMode
case object KafkaSasl extends SaslSetupMode
case object Both extends SaslSetupMode

/*
 * Trait used in SaslTestHarness and EndToEndAuthorizationTest to setup keytab and jaas files.
 */
trait SaslSetup {
  private val workDir = TestUtils.tempDir()
  private val kdcConf = MiniKdc.createConfig
  private var kdc: MiniKdc = null
<<<<<<< HEAD
  private var serverKeytabFile: Option[File] = None
  private var clientKeytabFile: Option[File] = None

  def startSasl(jaasSections: Seq[JaasSection]) {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val hasKerberos = jaasSections.exists(_.modules.exists {
      case _: Krb5LoginModule => true
      case _ => false
    })
    if (hasKerberos) {
      val (serverKeytabFile, clientKeytabFile) = maybeCreateEmptyKeytabFiles()
      kdc = new MiniKdc(kdcConf, workDir)
      kdc.start()
      kdc.createPrincipal(serverKeytabFile, JaasTestUtils.KafkaServerPrincipalUnqualifiedName + "/localhost")
      kdc.createPrincipal(clientKeytabFile,
        JaasTestUtils.KafkaClientPrincipalUnqualifiedName, JaasTestUtils.KafkaClientPrincipalUnqualifiedName2)
    }
    writeJaasConfigurationToFile(jaasSections)
    val hasZk = jaasSections.exists(_.modules.exists {
      case _: ZkDigestModule => true
      case _ => false
    })
    if (hasZk)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  /** Return a tuple with the path to the server keytab file and client keytab file */
  protected def maybeCreateEmptyKeytabFiles(): (File, File) = {
    if (serverKeytabFile.isEmpty)
      serverKeytabFile = Some(TestUtils.tempFile())
    if (clientKeytabFile.isEmpty)
      clientKeytabFile = Some(TestUtils.tempFile())
    (serverKeytabFile.get, clientKeytabFile.get)
  }

  protected def jaasSections(kafkaServerSaslMechanisms: Seq[String],
                             kafkaClientSaslMechanism: Option[String],
                             mode: SaslSetupMode = Both,
                             kafkaServerEntryName: String = JaasTestUtils.KafkaServerContextName): Seq[JaasSection] = {
    val hasKerberos = mode != ZkSasl &&
      (kafkaServerSaslMechanisms.contains("GSSAPI") || kafkaClientSaslMechanism.exists(_ == "GSSAPI"))
    if (hasKerberos)
      maybeCreateEmptyKeytabFiles()
    mode match {
      case ZkSasl => JaasTestUtils.zkSections
      case KafkaSasl =>
        Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms, serverKeytabFile),
          JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism, clientKeytabFile))
      case Both => Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms, serverKeytabFile),
        JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism, clientKeytabFile)) ++ JaasTestUtils.zkSections
=======
  private var serverKeytabFile: Option[File] = null
  private var clientKeytabFile: Option[File] = null

  def startSasl(mode: SaslSetupMode = Both, kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanism: Option[String]) {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val hasKerberos = mode != ZkSasl && (kafkaClientSaslMechanism == Some("GSSAPI") || kafkaServerSaslMechanisms.contains("GSSAPI"))
    if (hasKerberos) {
      val serverKeytabFile = TestUtils.tempFile()
      val clientKeytabFile = TestUtils.tempFile()
      this.clientKeytabFile = Some(clientKeytabFile)
      this.serverKeytabFile = Some(serverKeytabFile)
      kdc = new MiniKdc(kdcConf, workDir)
      kdc.start()
      kdc.createPrincipal(serverKeytabFile, JaasTestUtils.KafkaServerPrincipalUnqualifiedName + "/localhost")
      kdc.createPrincipal(clientKeytabFile, JaasTestUtils.KafkaClientPrincipalUnqualifiedName, JaasTestUtils.KafkaClientPrincipalUnqualifiedName2)
    } else {
      this.clientKeytabFile = None
      this.serverKeytabFile = None
    }
    setJaasConfiguration(mode, kafkaServerSaslMechanisms, kafkaClientSaslMechanism)
    if (mode == Both || mode == ZkSasl)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  protected def setJaasConfiguration(mode: SaslSetupMode, kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanism: Option[String]) {
    val jaasFile = mode match {
      case ZkSasl => JaasTestUtils.writeZkFile()
      case KafkaSasl => JaasTestUtils.writeKafkaFile(kafkaServerSaslMechanisms, kafkaClientSaslMechanism, serverKeytabFile, clientKeytabFile)
      case Both => JaasTestUtils.writeZkAndKafkaFiles(kafkaServerSaslMechanisms, kafkaClientSaslMechanism, serverKeytabFile, clientKeytabFile)
>>>>>>> origin/0.10.2
    }
  }

  private def writeJaasConfigurationToFile(jaasSections: Seq[JaasSection]) {
    val file = JaasTestUtils.writeJaasContextsToFile(jaasSections)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, file.getAbsolutePath)
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
  }

  def closeSasl() {
    if (kdc != null)
      kdc.stop()
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty("zookeeper.authProvider.1")
    Configuration.setConfiguration(null)
  }

<<<<<<< HEAD
  def kafkaServerSaslProperties(serverSaslMechanisms: Seq[String], interBrokerSaslMechanism: String): Properties = {
=======
  def kafkaServerSaslProperties(serverSaslMechanisms: Seq[String], interBrokerSaslMechanism: String) = {
>>>>>>> origin/0.10.2
    val props = new Properties
    props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, interBrokerSaslMechanism)
    props.put(SaslConfigs.SASL_ENABLED_MECHANISMS, serverSaslMechanisms.mkString(","))
    props
  }

<<<<<<< HEAD
  def kafkaClientSaslProperties(clientSaslMechanism: String, dynamicJaasConfig: Boolean = false): Properties = {
=======
  def kafkaClientSaslProperties(clientSaslMechanism: String, dynamicJaasConfig: Boolean = false) = {
>>>>>>> origin/0.10.2
    val props = new Properties
    props.put(SaslConfigs.SASL_MECHANISM, clientSaslMechanism)
    if (dynamicJaasConfig)
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism))
    props
  }

<<<<<<< HEAD
  def jaasClientLoginModule(clientSaslMechanism: String): String =
    JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile)

=======
  def jaasClientLoginModule(clientSaslMechanism: String): String = JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile)
>>>>>>> origin/0.10.2
}
