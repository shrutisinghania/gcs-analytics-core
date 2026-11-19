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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded, TTL-expiring pool of open {@link BlobReadSession} instances keyed by bucket and object
 * name, avoiding repeated {@code BidiReadObject} stream-open handshakes when the same object is
 * opened multiple times across splits or stages.
 */
final class BidiSessionPool implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BidiSessionPool.class);

  static final int DEFAULT_MAX_OBJECTS = 100;
  static final int DEFAULT_MAX_IDLE_PER_OBJECT = 16;
  static final long DEFAULT_EXPIRE_SECONDS = 600L;

  static final class ObjectKey {
    private final String bucketName;
    private final String objectName;

    private ObjectKey(String bucketName, String objectName) {
      this.bucketName = checkNotNull(bucketName, "bucketName cannot be null");
      this.objectName = checkNotNull(objectName, "objectName cannot be null");
    }

    static ObjectKey of(BlobId blobId) {
      return new ObjectKey(blobId.getBucket(), blobId.getName());
    }

    static ObjectKey of(String bucketName, String objectName) {
      return new ObjectKey(bucketName, objectName);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ObjectKey)) {
        return false;
      }
      ObjectKey that = (ObjectKey) o;
      return bucketName.equals(that.bucketName) && objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bucketName, objectName);
    }
  }

  static final class PooledSession {
    private final BlobId requestedBlobId;
    private final ApiFuture<BlobReadSession> sessionFuture;
    private volatile BlobReadSession resolvedSession;
    private volatile long lastAccessTimeNs;

    PooledSession(BlobId requestedBlobId, ApiFuture<BlobReadSession> sessionFuture) {
      this.requestedBlobId = checkNotNull(requestedBlobId, "requestedBlobId cannot be null");
      this.sessionFuture = checkNotNull(sessionFuture, "sessionFuture cannot be null");
      this.lastAccessTimeNs = System.nanoTime();
    }

    ApiFuture<BlobReadSession> getSessionFuture() {
      return sessionFuture;
    }

    @Nullable
    BlobReadSession getResolvedSession() {
      return resolvedSession;
    }

    void setResolvedSession(BlobReadSession session) {
      this.resolvedSession = session;
    }

    void touch() {
      this.lastAccessTimeNs = System.nanoTime();
    }

    long getLastAccessTimeNs() {
      return lastAccessTimeNs;
    }

    @Nullable
    Long getGeneration() {
      BlobReadSession session = resolvedSession;
      if (session != null) {
        BlobInfo info = session.getBlobInfo();
        if (info != null && info.getGeneration() != null) {
          return info.getGeneration();
        }
      }
      return requestedBlobId.getGeneration();
    }

    void closeActual() {
      BlobReadSession session = resolvedSession;
      resolvedSession = null;
      if (session != null) {
        try {
          session.close();
        } catch (Exception e) {
          LOG.debug("Exception closing pooled BlobReadSession for {}", requestedBlobId, e);
        }
        return;
      }
      if (sessionFuture.isDone()) {
        try {
          BlobReadSession completed = sessionFuture.get();
          if (completed != null) {
            completed.close();
          }
        } catch (Exception e) {
          LOG.debug(
              "Exception closing completed BlobReadSession future for {}", requestedBlobId, e);
        }
      } else {
        sessionFuture.cancel(true);
      }
    }
  }

  private final int maxIdlePerObject;
  private final long expireNanos;
  private final Cache<ObjectKey, ConcurrentLinkedQueue<PooledSession>> pool;
  private volatile boolean closed = false;

  BidiSessionPool() {
    this(DEFAULT_MAX_OBJECTS, DEFAULT_MAX_IDLE_PER_OBJECT, DEFAULT_EXPIRE_SECONDS);
  }

  @VisibleForTesting
  BidiSessionPool(int maxObjects, int maxIdlePerObject, long expireSeconds) {
    this.maxIdlePerObject = maxIdlePerObject;
    this.expireNanos = TimeUnit.SECONDS.toNanos(expireSeconds);
    this.pool =
        CacheBuilder.newBuilder()
            .maximumSize(maxObjects)
            .expireAfterAccess(Duration.ofSeconds(expireSeconds))
            .<ObjectKey, ConcurrentLinkedQueue<PooledSession>>removalListener(
                notification -> {
                  ConcurrentLinkedQueue<PooledSession> queue = notification.getValue();
                  if (queue != null) {
                    PooledSession entry;
                    while ((entry = queue.poll()) != null) {
                      entry.closeActual();
                    }
                  }
                })
            .build();
  }

  PooledSession acquire(Storage storage, BlobId blobId) {
    ObjectKey key = ObjectKey.of(blobId);
    ConcurrentLinkedQueue<PooledSession> queue = pool.getIfPresent(key);
    if (queue != null) {
      PooledSession candidate;
      while ((candidate = queue.poll()) != null) {
        if (closed || System.nanoTime() - candidate.getLastAccessTimeNs() > expireNanos) {
          candidate.closeActual();
          continue;
        }
        Long requestedGen = blobId.getGeneration();
        Long cachedGen = candidate.getGeneration();
        if (requestedGen != null && requestedGen > 0 && cachedGen != null) {
          if (!requestedGen.equals(cachedGen)) {
            candidate.closeActual();
            pool.invalidate(key);
            break;
          }
        }
        candidate.touch();
        return candidate;
      }
    }
    return new PooledSession(blobId, storage.blobReadSession(blobId));
  }

  void release(BlobId blobId, PooledSession session, boolean reusable) {
    if (!reusable || closed || session.getResolvedSession() == null) {
      session.closeActual();
      return;
    }
    session.touch();
    ObjectKey key = ObjectKey.of(blobId);
    try {
      ConcurrentLinkedQueue<PooledSession> queue = pool.get(key, ConcurrentLinkedQueue::new);
      if (closed || queue.size() >= maxIdlePerObject) {
        session.closeActual();
      } else {
        queue.offer(session);
      }
    } catch (ExecutionException e) {
      session.closeActual();
    }
  }

  void invalidate(GcsItemId itemId) {
    if (itemId != null && itemId.getObjectName().isPresent()) {
      pool.invalidate(ObjectKey.of(itemId.getBucketName(), itemId.getObjectName().get()));
    }
  }

  @Override
  public void close() {
    closed = true;
    pool.invalidateAll();
    pool.cleanUp();
  }
}
