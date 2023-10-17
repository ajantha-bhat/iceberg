/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public class TestBranchVisibilityForView extends BaseTestIceberg {

  private final TableIdentifier viewIdentifier1 = TableIdentifier.of("test-ns", "view1");
  private final TableIdentifier viewIdentifier2 = TableIdentifier.of("test-ns", "view2");
  private NessieCatalog testCatalog;

  public TestBranchVisibilityForView() {
    super("main");
  }

  @BeforeEach
  public void before() throws NessieNotFoundException, NessieConflictException {
    createView(catalog, viewIdentifier1);
    createView(catalog, viewIdentifier2);
    createBranch("test");
    testCatalog = initCatalog("test");
  }

  @AfterEach
  public void after() throws NessieNotFoundException, NessieConflictException {
    catalog.dropView(viewIdentifier1);
    catalog.dropView(viewIdentifier2);
    for (Reference reference : api.getAllReferences().get().getReferences()) {
      if (!reference.getName().equals("main")) {
        api.deleteBranch().branch((Branch) reference).delete();
      }
    }
    testCatalog = null;
  }

  @Test
  public void testBranchNoChange() {
    testCatalogEquality(catalog, testCatalog, true, true, () -> {});
  }

  /** Ensure catalogs can't see each others updates. */
  @Test
  public void testUpdateCatalogs() {
    testCatalogEquality(
        catalog, testCatalog, false, true, () -> replaceView(catalog, viewIdentifier1));

    testCatalogEquality(
        catalog, testCatalog, false, false, () -> replaceView(catalog, viewIdentifier2));
  }

  @Test
  public void testCatalogOnReference() {
    replaceView(catalog, viewIdentifier1);
    replaceView(testCatalog, viewIdentifier2);

    // catalog created with ref points to same catalog as above
    NessieCatalog refCatalog = initCatalog("test");
    testCatalogEquality(refCatalog, testCatalog, true, true, () -> {});

    // catalog created with hash points to same catalog as above
    NessieCatalog refHashCatalog = initCatalog("main");
    testCatalogEquality(refHashCatalog, catalog, true, true, () -> {});
  }

  @Test
  public void testCatalogWithViewNames() {
    replaceView(testCatalog, viewIdentifier2);

    String mainName = "main";

    // asking for view@branch gives expected regardless of catalog
    Assertions.assertThat(
            viewMetadataLocation(catalog, TableIdentifier.of("test-ns", "view1@test")))
        .isEqualTo(viewMetadataLocation(testCatalog, viewIdentifier1));

    // Asking for view@branch gives expected regardless of catalog.
    // Earlier versions used "view1@" + tree.getReferenceByName("main").getHash() before, but since
    // Nessie 0.8.2 the branch name became mandatory and specifying a hash within a branch is not
    // possible.
    Assertions.assertThat(
            viewMetadataLocation(catalog, TableIdentifier.of("test-ns", "view1@" + mainName)))
        .isEqualTo(viewMetadataLocation(testCatalog, viewIdentifier1));
  }

  @Test
  public void testConcurrentChanges() {
    NessieCatalog emptyTestCatalog = initCatalog("test");
    replaceView(testCatalog, viewIdentifier1);
    // Updating view with out of date hash. We expect this to succeed because of retry despite the
    // conflict.
    replaceView(emptyTestCatalog, viewIdentifier1);
  }

  @Test
  public void testMetadataLocation() throws Exception {
    String branch1 = "test";
    String branch2 = "branch-2";

    // commit on viewIdentifier1 on branch1
    NessieCatalog catalog = initCatalog(branch1);
    String metadataLocationOfCommit1 =
        ((BaseView) replaceView(catalog, viewIdentifier1))
            .operations()
            .current()
            .metadataFileLocation();

    createBranch(branch2, catalog.currentHash(), branch1);
    // commit on viewIdentifier1 on branch2
    catalog = initCatalog(branch2);
    String metadataLocationOfCommit2 =
        ((BaseView) replaceView(catalog, viewIdentifier1))
            .operations()
            .current()
            .metadataFileLocation();

    Assertions.assertThat(metadataLocationOfCommit2)
        .isNotNull()
        .isNotEqualTo(metadataLocationOfCommit1);

    catalog = initCatalog(branch1);
    // load viewIdentifier1 on branch1
    BaseView view = (BaseView) catalog.loadView(viewIdentifier1);
    // branch1's viewIdentifier1's metadata location must not have changed
    Assertions.assertThat(view.operations().current().metadataFileLocation())
        .isNotNull()
        .isNotEqualTo(metadataLocationOfCommit2);
  }

  @Test
  public void testWithRefAndHash() throws NessieConflictException, NessieNotFoundException {
    String testBranch = "testBranch";
    createBranch(testBranch);

    NessieCatalog nessieCatalog = initCatalog(testBranch);
    String hashBeforeNamespaceCreation = api.getReference().refName(testBranch).get().getHash();
    Namespace namespaceA = Namespace.of("a");
    Namespace namespaceAB = Namespace.of("a", "b");
    Assertions.assertThat(nessieCatalog.listNamespaces(namespaceAB)).isEmpty();

    createMissingNamespaces(
        nessieCatalog, Namespace.of(Arrays.copyOf(namespaceAB.levels(), namespaceAB.length() - 1)));
    nessieCatalog.createNamespace(namespaceAB);
    Assertions.assertThat(nessieCatalog.listNamespaces(namespaceAB)).isEmpty();
    Assertions.assertThat(nessieCatalog.listNamespaces(namespaceA)).containsExactly(namespaceAB);
    Assertions.assertThat(nessieCatalog.listViews(namespaceAB)).isEmpty();

    NessieCatalog catalogAtHash1 = initCatalog(testBranch, hashBeforeNamespaceCreation);
    Assertions.assertThat(catalogAtHash1.listNamespaces(namespaceAB)).isEmpty();
    Assertions.assertThat(catalogAtHash1.listViews(namespaceAB)).isEmpty();

    TableIdentifier identifier = TableIdentifier.of(namespaceAB, "view1");
    String hashBeforeViewCreation = nessieCatalog.currentHash();
    createView(nessieCatalog, identifier);
    Assertions.assertThat(nessieCatalog.listViews(namespaceAB)).hasSize(1);

    NessieCatalog catalogAtHash2 = initCatalog(testBranch, hashBeforeViewCreation);
    Assertions.assertThat(catalogAtHash2.listNamespaces(namespaceAB)).isEmpty();
    Assertions.assertThat(catalogAtHash2.listNamespaces(namespaceA)).containsExactly(namespaceAB);
    Assertions.assertThat(catalogAtHash2.listViews(namespaceAB)).isEmpty();

    // updates should not be possible
    Assertions.assertThatThrownBy(() -> createView(catalogAtHash2, identifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "You can only mutate contents when using a branch without a hash or timestamp.");
    Assertions.assertThat(catalogAtHash2.listViews(namespaceAB)).isEmpty();

    // updates should be still possible here
    nessieCatalog = initCatalog(testBranch);
    TableIdentifier identifier2 = TableIdentifier.of(namespaceAB, "view2");
    createView(nessieCatalog, identifier2);
    Assertions.assertThat(nessieCatalog.listViews(namespaceAB)).hasSize(2);
  }

  @Test
  public void testDifferentViewSameName() throws NessieConflictException, NessieNotFoundException {
    String branch1 = "branch1";
    String branch2 = "branch2";
    createBranch(branch1);
    createBranch(branch2);
    Schema schema1 =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    Schema schema2 =
        new Schema(
            Types.StructType.of(
                    required(1, "file_count", Types.IntegerType.get()),
                    required(2, "record_count", Types.LongType.get()))
                .fields());

    TableIdentifier identifier = TableIdentifier.of("db", "view1");

    NessieCatalog nessieCatalog = initCatalog(branch1);

    createMissingNamespaces(nessieCatalog, identifier);
    View view1 = createView(nessieCatalog, identifier, schema1);
    Assertions.assertThat(view1.schema().asStruct()).isEqualTo(schema1.asStruct());

    nessieCatalog = initCatalog(branch2);
    createMissingNamespaces(nessieCatalog, identifier);
    View view2 = createView(nessieCatalog, identifier, schema2);
    Assertions.assertThat(view2.schema().asStruct()).isEqualTo(schema2.asStruct());

    Assertions.assertThat(view1.location()).isNotEqualTo(view2.location());
  }

  private void testCatalogEquality(
      NessieCatalog catalog,
      NessieCatalog compareCatalog,
      boolean view1Equal,
      boolean view2Equal,
      ThrowingCallable callable) {
    String view1Location = viewMetadataLocation(compareCatalog, viewIdentifier1);
    String view2Location = viewMetadataLocation(compareCatalog, viewIdentifier2);

    try {
      callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

    String view1 = viewMetadataLocation(catalog, viewIdentifier1);
    String view2 = viewMetadataLocation(catalog, viewIdentifier2);

    AbstractStringAssert<?> assertion =
        Assertions.assertThat(view1)
            .describedAs(
                "View %s on ref %s should%s be equal to view %s on ref %s",
                viewIdentifier1.name(),
                catalog.currentRefName(),
                view1Equal ? "" : " not",
                viewIdentifier1.name(),
                compareCatalog.currentRefName());
    if (view1Equal) {
      assertion.isEqualTo(view1Location);
    } else {
      assertion.isNotEqualTo(view1Location);
    }

    assertion =
        Assertions.assertThat(view2)
            .describedAs(
                "View %s on ref %s should%s be equal to view %s on ref %s",
                viewIdentifier2.name(),
                catalog.currentRefName(),
                view2Equal ? "" : " not",
                viewIdentifier2.name(),
                compareCatalog.currentRefName());
    if (view2Equal) {
      assertion.isEqualTo(view2Location);
    } else {
      assertion.isNotEqualTo(view2Location);
    }
  }
}
