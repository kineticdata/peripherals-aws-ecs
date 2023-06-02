import java.text.SimpleDateFormat
import java.util.Date
plugins {
    id("net.nemerosa.versioning") version "2.14.0"
    java
    `maven-publish`
}
versioning {
  gitRepoRootDir = "../../"
}
tasks.processResources {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  val currentDate = SimpleDateFormat("yyyy-MM-dd").format(Date())
  from("src/main/resources"){
    filesMatching("**/*.version") {    
      expand(    
        "buildNumber" to versioning.info.build,
        "buildDate" to currentDate,    
        "timestamp" to System.currentTimeMillis(),    
        "version" to project.version    
      )    
    }
  }
}

repositories {
  mavenLocal()
  maven {
    url = uri("https://s3.amazonaws.com/maven-repo-public-kineticdata.com/releases")
  }

  maven {
    url = uri("s3://maven-repo-private-kineticdata.com/releases")
    authentication {
      create<AwsImAuthentication>("awsIm")
    }

  }

  maven {
    url = uri("https://s3.amazonaws.com/maven-repo-public-kineticdata.com/snapshots")
  }

  maven {
    url = uri("s3://maven-repo-private-kineticdata.com/snapshots")
    authentication {
      create<AwsImAuthentication>("awsIm")
    }
  }

  maven {
    url = uri("https://repo.maven.apache.org/maven2/")
  }

  maven {
    url = uri("https://repo.springsource.org/release/")
  }

}

dependencies {
    implementation("org.apache.httpcomponents:httpclient:4.5.1")
    implementation("com.kineticdata.agent:kinetic-agent-adapter:1.1.3")
    implementation("org.slf4j:slf4j-api:1.7.10")
    implementation("org.json:json:20230227")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.6.1")
    implementation("com.kineticdata.bridges.adapter:kinetic-bridgehub-adapter-amazonec2:2.0.0")
}

group = "com.kineticdata.bridges.adapter"
version = "1.0.1"
description = "kinetic-bridgehub-adapter-amazonecs"
java.sourceCompatibility = JavaVersion.VERSION_1_8

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
  repositories {
    maven {
      val releasesUrl = uri("s3://maven-repo-private-kineticdata.com/releases")
      val snapshotsUrl = uri("s3://maven-repo-private-kineticdata.com/snapshots")
      url = if (version.toString().endsWith("SNAPSHOT")) snapshotsUrl else releasesUrl
      authentication {
        create<AwsImAuthentication>("awsIm")
      }
    }
  }
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}
