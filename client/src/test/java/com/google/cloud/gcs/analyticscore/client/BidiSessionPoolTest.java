/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.Test;

class BidiSessionPoolTest {

  @Test
  void acquireAndRelease_reusableSession_reusesSessionWithoutReopening() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession session = mock(BlobReadSession.class);
    BlobId blobId = BlobId.of("bucket", "object.parquet");
    when(storage.blobReadSession(blobId)).thenReturn(ApiFutures.immediateFuture(session));

    try (BidiSessionPool pool = new BidiSessionPool()) {
      BidiSessionPool.PooledSession first = pool.acquire(storage, blobId);
      first.setResolvedSession(session);
      pool.release(blobId, first, /* reusable= */ true);

      BidiSessionPool.PooledSession second = pool.acquire(storage, blobId);
      assertThat(second).isSameInstanceAs(first);
      assertThat(second.getResolvedSession()).isSameInstanceAs(session);
      verify(storage, times(1)).blobReadSession(any(BlobId.class));
      verify(session, never()).close();

      pool.release(blobId, second, /* reusable= */ true);
    }

    verify(session, times(1)).close();
  }

  @Test
  void release_nonReusableSession_closesImmediatelyAndDoesNotPool() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession session1 = mock(BlobReadSession.class);
    BlobReadSession session2 = mock(BlobReadSession.class);
    BlobId blobId = BlobId.of("bucket", "object.parquet");
    when(storage.blobReadSession(blobId))
        .thenReturn(ApiFutures.immediateFuture(session1))
        .thenReturn(ApiFutures.immediateFuture(session2));

    try (BidiSessionPool pool = new BidiSessionPool()) {
      BidiSessionPool.PooledSession first = pool.acquire(storage, blobId);
      first.setResolvedSession(session1);
      pool.release(blobId, first, /* reusable= */ false);

      verify(session1, times(1)).close();

      BidiSessionPool.PooledSession second = pool.acquire(storage, blobId);
      assertThat(second).isNotSameInstanceAs(first);
      verify(storage, times(2)).blobReadSession(blobId);
      pool.release(blobId, second, /* reusable= */ false);
    }
  }

  @Test
  void acquire_generationMismatch_closesStaleSessionAndOpensFreshSession() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession oldSession = mock(BlobReadSession.class);
    BlobInfo oldInfo = mock(BlobInfo.class);
    when(oldInfo.getGeneration()).thenReturn(100L);
    when(oldSession.getBlobInfo()).thenReturn(oldInfo);

    BlobReadSession newSession = mock(BlobReadSession.class);
    BlobId unpinnedId = BlobId.of("bucket", "object.parquet");
    BlobId pinnedNewGenId = BlobId.of("bucket", "object.parquet", 200L);
    when(storage.blobReadSession(unpinnedId)).thenReturn(ApiFutures.immediateFuture(oldSession));
    when(storage.blobReadSession(pinnedNewGenId))
        .thenReturn(ApiFutures.immediateFuture(newSession));

    try (BidiSessionPool pool = new BidiSessionPool()) {
      BidiSessionPool.PooledSession first = pool.acquire(storage, unpinnedId);
      first.setResolvedSession(oldSession);
      pool.release(unpinnedId, first, /* reusable= */ true);

      BidiSessionPool.PooledSession second = pool.acquire(storage, pinnedNewGenId);
      assertThat(second).isNotSameInstanceAs(first);
      verify(oldSession, times(1)).close();
      verify(storage, times(1)).blobReadSession(pinnedNewGenId);
      pool.release(pinnedNewGenId, second, /* reusable= */ false);
    }
  }

  @Test
  void release_exceedingMaxIdlePerObject_closesExcessSession() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession session1 = mock(BlobReadSession.class);
    BlobReadSession session2 = mock(BlobReadSession.class);
    BlobId blobId = BlobId.of("bucket", "object.parquet");
    when(storage.blobReadSession(blobId))
        .thenReturn(ApiFutures.immediateFuture(session1))
        .thenReturn(ApiFutures.immediateFuture(session2));

    try (BidiSessionPool pool =
        new BidiSessionPool(
            /* maxObjects= */ 10, /* maxIdlePerObject= */ 1, /* expireSeconds= */ 60)) {
      BidiSessionPool.PooledSession first = pool.acquire(storage, blobId);
      BidiSessionPool.PooledSession second = pool.acquire(storage, blobId);
      first.setResolvedSession(session1);
      second.setResolvedSession(session2);

      pool.release(blobId, first, /* reusable= */ true);
      pool.release(blobId, second, /* reusable= */ true);

      verify(session1, never()).close();
      verify(session2, times(1)).close();
    }
    verify(session1, times(1)).close();
  }
}
