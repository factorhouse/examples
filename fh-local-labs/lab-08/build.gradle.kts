plugins {
    kotlin("jvm") version "2.1.20"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
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
    // Flink Core & Table AP
    compileOnly("org.apache.flink:flink-table-api-java-bridge:1.20.1")
    compileOnly("org.apache.flink:flink-table-planner-loader:1.20.1")
    compileOnly("org.apache.flink:flink-table-runtime:1.20.1")
    compileOnly("org.apache.flink:flink-streaming-java:1.20.1")
    compileOnly("org.apache.flink:flink-clients:1.20.1")
    compileOnly("org.apache.flink:flink-connector-files:1.20.1")
    // Flink Kafka and Avro
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-avro:1.20.1")
    implementation("org.apache.flink:flink-avro-confluent-registry:1.20.1")
    // Iceberg Connector
    compileOnly("org.apache.iceberg:iceberg-flink-runtime-1.20:1.8.1")
    compileOnly("org.apache.iceberg:iceberg-aws-bundle:1.8.1")
    // Logging - BUNDLED with your job JAR
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.5.13")
    // Test
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(17)
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

avro {
    setCreateSetters(true)
    setFieldVisibility("PUBLIC")
}

tasks.named("compileKotlin") {
    dependsOn("generateAvroJava")
}

sourceSets {
    named("main") {
        java.srcDirs("build/generated/avro/main")
        kotlin.srcDirs("src/main/kotlin")
    }
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("fh-local-flink-table-iceberg")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    environment("BOOTSTRAP", "localhost:9092")
    environment("REGISTRY_URL", "http://localhost:8081")
    environment("ICEBERG_REST_URL", "http://localhost:8181")
    environment("S3_ENDPOINT", "http://localhost:9000")

    val hadoopConfDir =
        layout.buildDirectory
            .dir("resources/main/hadoop")
            .get()
            .asFile.absolutePath
    environment("HADOOP_CONF_DIR", hadoopConfDir)
}

tasks.test {
    useJUnitPlatform()
}
