plugins {
    `java-library`
    id("org.openapi.generator") version "7.6.0"
}

group = "org.projectnessie.iceberg-catalog-migrator"
version = "0.3.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    implementation("org.openapitools:openapi-generator:5.2.0")
    implementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.google.code.gson:gson:2.12.0")
    implementation("org.apache.commons:commons-csv:1.13.0")



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