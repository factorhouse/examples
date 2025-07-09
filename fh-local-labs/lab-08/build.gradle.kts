plugins {
    kotlin("jvm") version "2.1.20"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
}

group = "io.factorhouse.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

dependencies {
    // Flink Core & Table API - included in the Flink cluster
    compileOnly("org.apache.flink:flink-streaming-java:1.20.1")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:1.20.1")
    compileOnly("org.apache.flink:flink-table-planner-loader:1.20.1")
    compileOnly("org.apache.flink:flink-table-runtime:1.20.1")
    compileOnly("org.apache.flink:flink-clients:1.20.1")
    compileOnly("org.apache.flink:flink-connector-files:1.20.1")

    // Flink Kafka and Avro dependencies
    implementation("org.apache.flink:flink-sql-connector-kafka:3.3.0-1.20")
    implementation("org.apache.flink:flink-sql-avro-confluent-registry:1.20.1")
    implementation("org.apache.flink:flink-avro:1.20.1")

    // Hive/Iceberg Dependencies - included in the Flink cluster
    compileOnly("org.apache.iceberg:iceberg-flink-runtime-1.20:1.8.1")
    compileOnly("org.apache.iceberg:iceberg-aws-bundle:1.8.1")
    compileOnly("org.apache.flink:flink-sql-connector-hive-3.1.3_2.12:1.20.1")
    compileOnly("org.apache.hadoop:hadoop-common:3.3.6")

    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.5.13")

    // Test
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(11)
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
    }
}

application {
    mainClass.set("io.factorhouse.demo.MainKt")
    applicationDefaultJvmArgs =
        listOf(
            "--add-opens=java.base/java.util=ALL-UNNAMED",
        )
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("fh-local-flink-table-iceberg")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()

    manifest {
        attributes["Main-Class"] = "io.factorhouse.demo.MainKt"
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("BOOTSTRAP", "localhost:9092")
    environment("INPUT_TOPIC", "orders")
    environment("REGISTRY_URL", "http://localhost:8081")
    environment("ICEBERG_CATALOG_NAME", "demo_ib")
    environment("HMS_ENDPOINT", "thrift://localhost:9093")
}

tasks.test {
    useJUnitPlatform()
}
