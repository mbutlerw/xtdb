// Deliberately NOT `java-library`: the root build's shared test config assumes clojurephant
// (`devRuntimeOnly` et al), which this pure-Kotlin test-infra module doesn't want. It declares
// its own minimal test setup instead.
plugins {
    alias(libs.plugins.kotlin.jvm)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    // for the TeardownStallProbe SPI (xtdb#5711) — api is the lowest module and already on every
    // test runtime, so this adds nothing to the aggregate classpath.
    implementation(project(":xtdb-api"))

    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.coroutines.debug)
    implementation(libs.junit.platform.launcher)

    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}
