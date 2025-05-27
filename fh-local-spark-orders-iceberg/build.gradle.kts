import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.factorhouse.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val scalaBinaryVersion = "2.12"
val sparkVersion = "3.5.0"

dependencies {
    // Dependencies for Kafka + Avro
    implementation("za.co.absa:abris_${scalaBinaryVersion}:6.4.1")
    implementation("io.confluent:kafka-avro-serializer:6.2.1")
    implementation("io.confluent:kafka-schema-registry-client:6.2.1")
    implementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")
    implementation("org.apache.spark:spark-avro_${scalaBinaryVersion}:${sparkVersion}")

    // Spark Core and SQL APIs - typically compileOnly if submitting to a Spark cluster
    // as the cluster provides these. If you bundle them, you might get version conflicts.
    compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
    compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
}

tasks.withType<ShadowJar> {
    archiveBaseName.set("fh-local-spark-orders-iceberg")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()
}

tasks.named("build") {
   dependsOn(tasks.shadowJar)
}