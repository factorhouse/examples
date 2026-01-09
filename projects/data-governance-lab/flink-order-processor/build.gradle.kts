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

val localRunClasspath by configurations.creating {
    extendsFrom(configurations.implementation.get(), configurations.compileOnly.get(), configurations.runtimeOnly.get())
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

val flinkVersion = "1.20.1"
val log4jVersion = "2.17.1"

dependencies {
    // Flink Dependencies
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")
    // 'testImplementation' makes Flink available for test source compilation and execution.
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")
    // Kafka and Avro
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-avro:$flinkVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    // Logging
    compileOnly("org.slf4j:slf4j-api:1.7.36")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.14.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.14.1")
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
        exclude(dependency("org.slf4j:slf4j-log4j12"))
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("BOOTSTRAP", "localhost:9092")
    environment("REGISTRY_URL", "http://localhost:8081")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
