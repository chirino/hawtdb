/**
 * Copyright (C) 2009-2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.fusesource.scalate.RenderContext

package

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Website {

  val project_name= "HawtDB"
  val project_slogan= "A Powerful Key/Value Store."
  val project_id= "hawtdb"
  val project_jira_key= "HAWTJNI"
  val project_issue_url= "http://www.assembla.com/spaces/hawtdb/support/tickets"
  val project_forums_url= "http://groups.google.com/group/hawtdb"
  val project_wiki_url= "http://fusesource.com/wiki/display/HAWTDB"
  val project_logo= "/images/project-logo.png"
  val project_snapshot_version= "1.7-SNAPSHOT"
  val project_version= "1.6"
  val project_versions = List(
        project_version,
        "1.5",
        "1.4",
        "1.3",
        "1.2",
        "1.1",
        "1.0"
        )  

  val project_keywords= "db,btree,hash,key,value,nosql,embedded,java,jvm"

  // -------------------------------------------------------------------
  val github_page= "http://github.com/fusesource/hawtdb"
  val git_user_url= "git://github.com/fusesource/hawtdb.git"
  val git_commiter_url= "git@github.com:fusesources/hawtdb.git"

  val project_maven_groupId= "org.fusesource.hawtdb"
  val project_maven_artifactId= "hawtdb"
  val website_base_url= "http://hawtdb.fusesource.org"
  val api_base = website_base_url+"/versions/"+project_version+"/website/documentation/api/hawtdb/org/fusesource/hawtdb"
}