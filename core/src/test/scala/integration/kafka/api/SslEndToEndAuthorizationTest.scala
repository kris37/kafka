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

import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Before

class SslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  override protected def securityProtocol = SecurityProtocol.SSL
  this.serverConfig.setProperty(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
  override val clientPrincipal = "O=A client,CN=localhost"
  override val kafkaPrincipal = "O=A server,CN=localhost"

  @Before
  override def setUp {
<<<<<<< HEAD
    startSasl(jaasSections(List.empty, None, ZkSasl))
=======
    startSasl(ZkSasl, List.empty, None)
>>>>>>> origin/0.10.2
    super.setUp
  }
}
