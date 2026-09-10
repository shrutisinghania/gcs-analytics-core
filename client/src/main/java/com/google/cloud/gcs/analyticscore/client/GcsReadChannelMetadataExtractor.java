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
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

  private static final ImmutableList<String> METADATA_METHOD_NAMES =
      ImmutableList.of(
          "getResolvedObject", "getObject", "getBlobInfo", "getBlob", "getStorageObject");

  private static final ImmutableList<String> METADATA_FIELD_NAMES =
      ImmutableList.of("result", "blobInfo", "storageObject", "object");

  private static final ImmutableList<String> WRAPPER_FIELD_NAMES =
      ImmutableList.of(
          "reader",
          "delegate",
          "channel",
          "wrapped",
          "in",
          "readChannel",
          "lazyReadChannel",
          "session");

  private GcsReadChannelMetadataExtractor() {}

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

  @Nullable
  static ExtractedMetadata extract(@Nullable Object channelOrTarget) {
    if (channelOrTarget == null) {
      return null;
    }
    return resolveMetadata(channelOrTarget, 0);
  }

  @Nullable
  private static ExtractedMetadata tryExtractMetadata(@Nullable Object obj) {
    if (obj == null) {
      return null;
    }
    long size = extractLongProperty(obj, "getSize", "size");
    if (size < 0) {
      return null;
    }
    long generation = extractLongProperty(obj, "getGeneration", "generation");
    return new ExtractedMetadata(size, generation);
  }

  @Nullable
  private static ExtractedMetadata resolveMetadata(@Nullable Object target, int depth) {
    if (target == null || depth > 5) {
      return null;
    }
    Class<?> clazz = target.getClass();

    while (clazz != null && clazz != Object.class) {
      for (String methodName : METADATA_METHOD_NAMES) {
        Method method = findDeclaredMethod(clazz, methodName);
        if (method == null) {
          continue;
        }
        try {
          method.setAccessible(true);
          Object raw = method.invoke(target);
          ExtractedMetadata metadata = tryExtractMetadata(resolveFutureIfNeeded(raw));
          if (metadata != null) {
            return metadata;
          }
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug("Failed invoking method {} on {}", methodName, clazz.getName(), e);
        }
      }
      for (String fieldName : METADATA_FIELD_NAMES) {
        Field field = findDeclaredField(clazz, fieldName);
        if (field == null) {
          continue;
        }
        try {
          field.setAccessible(true);
          Object raw = field.get(target);
          ExtractedMetadata metadata = tryExtractMetadata(resolveFutureIfNeeded(raw));
          if (metadata != null) {
            return metadata;
          }
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug("Failed reading field {} on {}", fieldName, clazz.getName(), e);
        }
      }
      for (String wrapperFieldName : WRAPPER_FIELD_NAMES) {
        Field field = findDeclaredField(clazz, wrapperFieldName);
        if (field == null) {
          continue;
        }
        try {
          field.setAccessible(true);
          Object inner = field.get(target);
          if (inner != null && inner != target) {
            ExtractedMetadata metadata = resolveMetadata(inner, depth + 1);
            if (metadata != null) {
              return metadata;
            }
          }
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug(
              "Failed unwrapping wrapper field {} on {}", wrapperFieldName, clazz.getName(), e);
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  @Nullable
  private static Method findDeclaredMethod(Class<?> clazz, String methodName) {
    try {
      return clazz.getDeclaredMethod(methodName);
    } catch (NoSuchMethodException ignored) {
      return null;
    }
  }

  @Nullable
  private static Field findDeclaredField(Class<?> clazz, String fieldName) {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException ignored) {
      return null;
    }
  }

  @Nullable
  private static Object resolveFutureIfNeeded(@Nullable Object obj) {
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

  private static boolean isExcludedFallbackTarget(Object target) {
    return target instanceof Map
        || target instanceof Collection
        || target instanceof CharSequence
        || target instanceof Number
        || target.getClass().getName().startsWith("java.util.")
        || target.getClass().getName().startsWith("com.google.common.collect.");
  }

  private static long extractLongProperty(
      Object target, String primaryGetter, String fallbackGetter) {
    Method primaryMethod = findMethod(target.getClass(), primaryGetter);
    if (primaryMethod != null) {
      long val = invokeLongGetter(target, primaryMethod);
      if (val >= 0) {
        return val;
      }
    }
    if (isExcludedFallbackTarget(target)) {
      return -1L;
    }
    Method fallbackMethod = findMethod(target.getClass(), fallbackGetter);
    if (fallbackMethod != null) {
      long val = invokeLongGetter(target, fallbackMethod);
      if (val >= 0) {
        return val;
      }
    }
    return -1L;
  }

  @Nullable
  private static Method findMethod(Class<?> clazz, String methodName) {
    try {
      return clazz.getMethod(methodName);
    } catch (NoSuchMethodException | SecurityException ignored) {
      // Fall through to declared method search
    }
    Class<?> current = clazz;
    while (current != null && current != Object.class) {
      Method m = findDeclaredMethod(current, methodName);
      if (m != null) {
        return m;
      }
      current = current.getSuperclass();
    }
    return null;
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
