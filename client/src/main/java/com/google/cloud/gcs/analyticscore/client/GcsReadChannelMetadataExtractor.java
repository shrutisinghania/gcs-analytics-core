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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to extract metadata from GCS SDK {@link ReadChannel} instances using reflection.
 */
final class GcsReadChannelMetadataExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(GcsReadChannelMetadataExtractor.class);

  /**
   * Generic, best-effort accessors tried on unknown channel implementations. {@code
   * getResolvedObject} is listed before {@code getObject} because on the SDK's {@code
   * BaseStorageReadChannel} the former returns the already-decoded object, while the latter returns
   * an {@code ApiFuture} that is only resolved once a response has been received.
   */
  private static final ImmutableList<String> METADATA_METHOD_NAMES =
      ImmutableList.of(
          "getResolvedObject", "getObject", "getBlobInfo", "getBlob", "getStorageObject");

  private static final ImmutableList<String> METADATA_FIELD_NAMES =
      ImmutableList.of("blobInfo", "storageObject", "object", "result");

  /**
   * Package-private SDK interface implemented by every SDK read channel ({@code BlobReadChannelV2},
   * {@code GrpcBlobReadChannel}, ...). It exposes {@code ApiFuture<BlobInfo> getObject()}, which is
   * the typed, version-stable way of obtaining the object metadata.
   */
  private static final String SDK_READ_CHANNEL_INTERFACE_NAME =
      "com.google.cloud.storage.StorageReadChannel";

  private static final String SDK_READ_CHANNEL_METADATA_METHOD_NAME = "getObject";

  /**
   * The OpenTelemetry decorator wraps the real SDK channel in a {@code reader} field. Verified
   * against google-cloud-storage 2.72.0.
   */
  private static final String SDK_CHANNEL_DECORATOR_SIMPLE_NAME = "OtelDecoratedReadChannel";

  private static final String SDK_CHANNEL_DECORATOR_FIELD_NAME = "reader";

  @Nullable private static final Method SDK_GET_OBJECT_METHOD = lookupSdkGetObjectMethod();

  private GcsReadChannelMetadataExtractor() {}

  @Nullable
  private static Method lookupSdkGetObjectMethod() {
    try {
      Class<?> sdkInterface = Class.forName(SDK_READ_CHANNEL_INTERFACE_NAME);
      Method method = sdkInterface.getMethod(SDK_READ_CHANNEL_METADATA_METHOD_NAME);
      method.setAccessible(true);
      return method;
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug("SDK read channel interface not available; using generic extraction only", e);
      return null;
    }
  }

  static final class ExtractedMetadata {
    private final long size;
    private final long generation;

    ExtractedMetadata(long size, long generation) {
      this.size = size;
      this.generation = generation;
    }

    long getSize() {
      return size;
    }

    long getGeneration() {
      return generation;
    }
  }

  /**
   * Extracts size and generation from an SDK read channel after at least one read response has been
   * received. Returns {@code null} when the channel does not (yet) expose the metadata; this never
   * triggers a network request.
   */
  @Nullable
  static ExtractedMetadata extract(@Nullable ReadChannel sdkChannel) {
    if (sdkChannel == null) {
      return null;
    }
    return extractFromTarget(unwrapSdkDecorator(sdkChannel));
  }

  @Nullable
  @VisibleForTesting
  static ExtractedMetadata extractFromTarget(@Nullable Object target) {
    if (target == null) {
      return null;
    }

    ExtractedMetadata typedMetadata = extractFromSdkChannel(target);
    if (typedMetadata != null) {
      return typedMetadata;
    }

    Object resolvedMetadata = resolveMetadataObject(target);
    if (resolvedMetadata == null) {
      return null;
    }
    long extractedSize = extractLongProperty(resolvedMetadata, "getSize", "size");
    if (extractedSize < 0) {
      return null;
    }
    long extractedGeneration = extractLongProperty(resolvedMetadata, "getGeneration", "generation");
    return new ExtractedMetadata(extractedSize, extractedGeneration);
  }

  private static Object unwrapSdkDecorator(Object channel) {
    if (!SDK_CHANNEL_DECORATOR_SIMPLE_NAME.equals(channel.getClass().getSimpleName())) {
      return channel;
    }
    Object delegate = readDeclaredField(channel, SDK_CHANNEL_DECORATOR_FIELD_NAME);
    return delegate != null ? delegate : channel;
  }

  /** Typed fast path for channels implementing the SDK's {@code StorageReadChannel} interface. */
  @Nullable
  private static ExtractedMetadata extractFromSdkChannel(Object target) {
    if (SDK_GET_OBJECT_METHOD == null
        || !SDK_GET_OBJECT_METHOD.getDeclaringClass().isInstance(target)) {
      return null;
    }
    try {
      Object resolved = resolveFutureIfNeeded(SDK_GET_OBJECT_METHOD.invoke(target));
      if (!(resolved instanceof BlobInfo)) {
        return null;
      }
      BlobInfo blobInfo = (BlobInfo) resolved;
      Long size = blobInfo.getSize();
      if (size == null || size < 0) {
        return null;
      }
      Long generation = blobInfo.getGeneration();
      return new ExtractedMetadata(size, generation == null ? -1L : generation);
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug("Failed reading the object metadata from {}", target.getClass().getName(), e);
      return null;
    }
  }

  @Nullable
  private static Object readDeclaredField(Object target, String fieldName) {
    try {
      Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug("Failed reading field {} on {}", fieldName, target.getClass().getName(), e);
      return null;
    }
  }

  private static boolean hasValidMetadata(@Nullable Object obj) {
    if (obj == null) {
      return false;
    }
    return extractLongProperty(obj, "getSize", "size") >= 0;
  }

  @Nullable
  private static Object resolveMetadataObject(@Nullable Object target) {
    if (target == null) {
      return null;
    }
    Class<?> clazz = target.getClass();

    while (clazz != null && clazz != Object.class) {
      for (String methodName : METADATA_METHOD_NAMES) {
        try {
          Method method = clazz.getDeclaredMethod(methodName);
          method.setAccessible(true);
          Object raw = method.invoke(target);
          Object resolvedMetadata = resolveFutureIfNeeded(raw);
          if (hasValidMetadata(resolvedMetadata)) {
            return resolvedMetadata;
          }
        } catch (NoSuchMethodException ignored) {
          // Expected: the candidate accessor does not exist on this class; try the next one.
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug("Failed invoking method {} on {}", methodName, clazz.getName(), e);
        }
      }
      for (String fieldName : METADATA_FIELD_NAMES) {
        try {
          Field field = clazz.getDeclaredField(fieldName);
          field.setAccessible(true);
          Object raw = field.get(target);
          Object resolvedMetadata = resolveFutureIfNeeded(raw);
          if (hasValidMetadata(resolvedMetadata)) {
            return resolvedMetadata;
          }
        } catch (NoSuchFieldException ignored) {
          // Expected: the candidate field does not exist on this class; try the next one.
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug("Failed reading field {} on {}", fieldName, clazz.getName(), e);
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  @Nullable
  private static Object resolveFutureIfNeeded(@Nullable Object obj)
      throws ReflectiveOperationException {
    if (obj == null) {
      return null;
    }
    if (!(obj instanceof Future)) {
      return obj;
    }
    Future<?> future = (Future<?>) obj;
    if (!future.isDone()) {
      return null;
    }
    try {
      return future.get();
    } catch (CancellationException | ExecutionException e) {
      LOG.debug("Future execution failed or was cancelled", e);
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private static long extractLongProperty(
      Object target, String primaryGetter, String fallbackGetter) {
    long value =
        Arrays.stream(target.getClass().getMethods())
            .filter(m -> m.getParameterCount() == 0 && m.getName().equals(primaryGetter))
            .mapToLong(m -> invokeLongGetter(target, m))
            .filter(v -> v >= 0)
            .findFirst()
            .orElse(-1L);
    if (value >= 0) {
      return value;
    }
    if (target instanceof Map || target instanceof Collection) {
      return -1L;
    }
    return Arrays.stream(target.getClass().getMethods())
        .filter(m -> m.getParameterCount() == 0 && m.getName().equals(fallbackGetter))
        .mapToLong(m -> invokeLongGetter(target, m))
        .filter(v -> v >= 0)
        .findFirst()
        .orElse(-1L);
  }

  private static long invokeLongGetter(Object target, Method method) {
    try {
      method.setAccessible(true);
      Object value = method.invoke(target);
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug(
          "Getter invocation failed or inaccessible for method {} on target {}",
          method.getName(),
          target);
    }
    return -1L;
  }
}
