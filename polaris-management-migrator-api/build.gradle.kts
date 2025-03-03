plugins {
    id("java")
    `java-library`
    alias(libs.plugins.openapi.generator.gradle.plugin)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21)) // Set the compilation JDK to 21
    }
}

group = "org.projectnessie.iceberg-catalog-migrator"
version = "0.3.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.mockito)


    implementation(project(":iceberg-catalog-migrator-api"))
    implementation(libs.openapi.generator)
    implementation(libs.jakarta.annotation)
    implementation(libs.okhttp)
    implementation(libs.gson)
    implementation(libs.apache.commons.csv)
    implementation(libs.iceberg.spark.runtime)

    implementation(libs.hadoop.common) {
        exclude("org.apache.avro", "avro")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("javax.servlet", "servlet-api")
        exclude("com.google.code.gson", "gson")
        exclude("commons-beanutils")
    }

    compileOnly(libs.immutables.value.annotations)
    annotationProcessor(libs.immutables.value.processor)

    implementation(libs.slf4j)
}

tasks.register<org.openapitools.generator.gradle.plugin.tasks.GenerateTask>("generatePolarisManagementClient") {
    inputSpec.set("$projectDir/../polaris/spec/polaris-management-service.yml")
    generatorName.set("java")
    outputDir.set("$buildDir/generated")
    apiPackage.set("com.snowflake.polaris.management.client")
    modelPackage.set("org.apache.polaris.core.admin.model")
    removeOperationIdPrefix.set(true)

    globalProperties.set(
        mapOf(
            "apis" to "",
            "models" to "",
            "supportingFiles" to "",
            "apiDocs" to "false",
            "modelTests" to "false"
        )
    )

    additionalProperties.set(
        mapOf(
            "apiNamePrefix" to "PolarisManagement",
            "apiNameSuffix" to "Api",
            "metricsPrefix" to "polaris.management"
        )
    )

    configOptions.set(
        mapOf(
            "library" to "native",
            "sourceFolder" to "src/main/java",
            "useJakartaEe" to "true",
            "useBeanValidation" to "false",
            "openApiNullable" to "false",
            "useRuntimeException" to "true",
            "supportUrlQuery" to "false"
        )
    )

    importMappings.set(
        mapOf(
            "AbstractOpenApiSchema" to "org.apache.polaris.core.admin.model.AbstractOpenApiSchema",
            "AddGrantRequest" to "org.apache.polaris.core.admin.model.AddGrantRequest",
            "AwsStorageConfigInfo" to "org.apache.polaris.core.admin.model.AwsStorageConfigInfo",
            "AzureStorageConfigInfo" to "org.apache.polaris.core.admin.model.AzureStorageConfigInfo",
            "Catalog" to "org.apache.polaris.core.admin.model.Catalog",
            "CatalogGrant" to "org.apache.polaris.core.admin.model.CatalogGrant",
            "CatalogPrivilege" to "org.apache.polaris.core.admin.model.CatalogPrivilege",
            "CatalogProperties" to "org.apache.polaris.core.admin.model.CatalogProperties",
            "CatalogRole" to "org.apache.polaris.core.admin.model.CatalogRole",
            "CatalogRoles" to "org.apache.polaris.core.admin.model.CatalogRoles",
            "Catalogs" to "org.apache.polaris.core.admin.model.Catalogs",
            "CreateCatalogRequest" to "org.apache.polaris.core.admin.model.CreateCatalogRequest",
            "CreateCatalogRoleRequest" to "org.apache.polaris.core.admin.model.CreateCatalogRoleRequest",
            "CreatePrincipalRequest" to "org.apache.polaris.core.admin.model.CreatePrincipalRequest",
            "CreatePrincipalRoleRequest" to "org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest",
            "ExternalCatalog" to "org.apache.polaris.core.admin.model.ExternalCatalog",
            "FileStorageConfigInfo" to "org.apache.polaris.core.admin.model.FileStorageConfigInfo",
            "GcpStorageConfigInfo" to "org.apache.polaris.core.admin.model.GcpStorageConfigInfo",
            "GrantCatalogRoleRequest" to "org.apache.polaris.core.admin.model.GrantCatalogRoleRequest",
            "GrantPrincipalRoleRequest" to "org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest",
            "GrantResource" to "org.apache.polaris.core.admin.model.GrantResource",
            "GrantResources" to "org.apache.polaris.core.admin.model.GrantResources",
            "NamespaceGrant" to "org.apache.polaris.core.admin.model.NamespaceGrant",
            "NamespacePrivilege" to "org.apache.polaris.core.admin.model.NamespacePrivilege",
            "PolarisCatalog" to "org.apache.polaris.core.admin.model.PolarisCatalog",
            "Principal" to "org.apache.polaris.core.admin.model.Principal",
            "PrincipalRole" to "org.apache.polaris.core.admin.model.PrincipalRole",
            "PrincipalRoles" to "org.apache.polaris.core.admin.model.PrincipalRoles",
            "PrincipalWithCredentials" to "org.apache.polaris.core.admin.model.PrincipalWithCredentials",
            "PrincipalWithCredentialsCredentials" to "org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials",
            "Principals" to "org.apache.polaris.core.admin.model.Principals",
            "RevokeGrantRequest" to "org.apache.polaris.core.admin.model.RevokeGrantRequest",
            "StorageConfigInfo" to "org.apache.polaris.core.admin.model.StorageConfigInfo",
            "TableGrant" to "org.apache.polaris.core.admin.model.TableGrant",
            "TablePrivilege" to "org.apache.polaris.core.admin.model.TablePrivilege",
            "UpdateCatalogRequest" to "org.apache.polaris.core.admin.model.UpdateCatalogRequest",
            "UpdateCatalogRoleRequest" to "org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest",
            "UpdatePrincipalRequest" to "org.apache.polaris.core.admin.model.UpdatePrincipalRequest",
            "UpdatePrincipalRoleRequest" to "org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest",
            "ViewGrant" to "org.apache.polaris.core.admin.model.ViewGrant",
            "ViewPrivilege" to "org.apache.polaris.core.admin.model.ViewPrivilege"
        )
    )
}

tasks.named("compileJava") {
    dependsOn("generatePolarisManagementClient")
}

sourceSets.main {
    java.srcDir("$buildDir/generated/src/main/java")
}

tasks.test {
    useJUnitPlatform()
}