import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test

plugins {
    kotlin("jvm") version "2.2.20"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.factorhouse.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

val flinkVersion = "1.20.1"
val log4jVersion = "2.17.1"
val icebergVersion = "1.8.1"
val hadoopVersion = "3.3.6"

configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "org.slf4j", module = "slf4j-reload4j")
    exclude(group = "log4j", module = "log4j")
}

val localRunClasspath by configurations.creating {
    extendsFrom(configurations.implementation.get(), configurations.compileOnly.get(), configurations.runtimeOnly.get())
}

dependencies {
    // Flink Dependencies
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-runtime:$flinkVersion")
    // 'testImplementation' makes Flink available for test source compilation and execution.
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    // Kafka and Avro
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-avro:$flinkVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    // Iceberg & Hive
    compileOnly("org.apache.iceberg:iceberg-flink-runtime-1.20:$icebergVersion")
    compileOnly("org.apache.iceberg:iceberg-aws-bundle:$icebergVersion")
    compileOnly("org.apache.hadoop:hadoop-common:$hadoopVersion")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
    compileOnly("org.apache.hadoop:hadoop-aws:$hadoopVersion")
    compileOnly("org.apache.flink:flink-sql-connector-hive-3.1.3_2.12:$flinkVersion")
    // ClickHouse
    implementation("com.clickhouse.flink:flink-connector-clickhouse-1.17:0.1.3:all")
    // 'testImplementation' makes Flink available for test source compilation and execution.
    testImplementation("org.apache.iceberg:iceberg-flink-runtime-1.20:$icebergVersion")
    testImplementation("org.apache.iceberg:iceberg-aws-bundle:$icebergVersion")
    testImplementation("org.apache.hadoop:hadoop-common:3.3.6")
    testImplementation("org.apache.hadoop:hadoop-common:$hadoopVersion")
    testImplementation("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
    testImplementation("org.apache.hadoop:hadoop-aws:$hadoopVersion")
    testImplementation("org.apache.flink:flink-sql-connector-hive-3.1.3_2.12:$flinkVersion")
    // Logging
    runtimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.14.1")
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("io.factorhouse.demo.MainKt")
}

tasks.named<JavaExec>("run") {
    // Classpath = All library dependencies + The application's compiled code.
    classpath = localRunClasspath + sourceSets.main.get().output
}

tasks.withType<ShadowJar> {
    archiveBaseName.set(rootProject.name)
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()

    dependencies {
        exclude(dependency("org.apache.logging.log4j:.*"))
        exclude(dependency("org.slf4j:.*"))
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("BOOTSTRAP", "localhost:9092")
    environment("REGISTRY_URL", "http://localhost:8081")
    environment("HMS_ENDPOINT", "thrift://localhost:9083")
    environment("S3_ENDPOINT", "http://localhost:9000")
    environment("CH_ENDPOINT", "http://localhost:8123")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
