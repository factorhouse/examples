import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.factorhouse.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val scalaBinaryVersion = "2.12"
val sparkVersion = "3.5.0"
val openLineageVersion = "1.15.0"

dependencies {
    // Spark Core and SQL APIs
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    // OpenLineage
    implementation("io.openlineage:openlineage-spark:$openLineageVersion")
    // Required transitive dependencies for the OpenLineage client.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-core:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.15.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_2.12:2.15.2")
}

tasks.withType<ShadowJar> {
    archiveBaseName.set("spark-supplier-stats")
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    // The relocation rule is critical to avoid classpath conflicts with Spark's own libraries.
    relocate("com.fasterxml.jackson", "io.openlineage.spark.shaded.jackson")
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn(tasks.shadowJar)
}
