package io.ledgerstream.app.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

final class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "io.ledgerstream";
    private static JavaClasses importedClasses;

    @BeforeAll
    static void importProductionClasses() {
        importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(BASE_PACKAGE);
    }

    @Test
    void domainMustOnlyDependOnJavaStandardLibraryAndDomain() {
        ArchRule rule = classes()
                .that().resideInAPackage("io.ledgerstream.domain..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage(
                        "java..",
                        "io.ledgerstream.domain.."
                );

        rule.check(importedClasses);
    }

    @Test
    void applicationMustBeFrameworkFreeAndOnlyDependOnDomainAndApplication() {
        ArchRule rule = classes()
                .that().resideInAPackage("io.ledgerstream.application..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage(
                        "java..",
                        "io.ledgerstream.domain..",
                        "io.ledgerstream.application.."
                );

        rule.check(importedClasses);
    }

    @Test
    void inboundAdaptersMustNotDependOnOutboundAdaptersOrAppCompositionRoot() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.ledgerstream.adapter.in..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "io.ledgerstream.adapter.out..",
                        "io.ledgerstream.app.."
                );

        rule.check(importedClasses);
    }

    @Test
    void outboundAdaptersMustNotDependOnInboundAdaptersOrAppCompositionRoot() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.ledgerstream.adapter.out..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "io.ledgerstream.adapter.in..",
                        "io.ledgerstream.app.."
                );

        rule.check(importedClasses);
    }

    @Test
    void productionCodeMustNeverDependOnLombokOrJpa() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.ledgerstream..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "lombok..",
                        "jakarta.persistence..",
                        "javax.persistence..",
                        "org.hibernate.."
                );

        rule.check(importedClasses);
    }

    @Test
    void topLevelModulesMustBeFreeOfCyclicDependencies() {
        ArchRule rule = slices()
                .matching("io.ledgerstream.(*)..")
                .should().beFreeOfCycles();

        rule.check(importedClasses);
    }
}
