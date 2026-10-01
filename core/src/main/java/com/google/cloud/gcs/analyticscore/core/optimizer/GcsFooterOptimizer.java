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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/** A {@link FormatOptimizer} that caches and serves GCS object footers (e.g., for Parquet). */
public class GcsFooterOptimizer implements FormatOptimizer {

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB
  private static final Set<String> FOOTER_OPTIMIZABLE_EXTENSIONS = Set.of(".parquet", ".orc");

  /**
   * Upper bound on the bytes fetched ahead of the first read when the object size is still unknown.
   * Keeps the speculative window bounded even if the configured footer prefetch sizes are large.
   */
  private static final long MAX_SPECULATIVE_PREFETCH_SIZE = 4L * 1024 * 1024; // 4 MiB

  private final GcsReadOptions readOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId gcsItemId;
  private long fileSize = -1;
  private long prefetchSize = -1;

  /**
   * Bytes held locally for this stream, covering {@code [localBufferStartPosition,
   * localBufferStartPosition + localFooterBuffer.limit())}. Usually the cached footer, but it may
   * also be a speculative window read before the size was known.
   */
  private ByteBuffer localFooterBuffer;

  private long localBufferStartPosition = -1;
  private boolean speculativeReadAttempted = false;

  public GcsFooterOptimizer(GcsReadOptions readOptions, Telemetry telemetry) {
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return readOptions.isFooterPrefetchEnabled()
        && itemId
            .getObjectName()
            .map(
                name ->
                    FOOTER_OPTIMIZABLE_EXTENSIONS.stream()
                        .anyMatch(ext -> name.toLowerCase().endsWith(ext)))
            .orElse(false);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1) {
      resolveFileSizeFromItemInfo(source);
    }
    if (fileSize == -1) {
      if (!speculativeReadAttempted) {
        return readWithUnknownFileSize(position, dst, source);
      }
      resolveFileSizeExplicitly(source);
    }

    // Checked before the footer-range guard: a window prefetched while the size was unknown may
    // legitimately start before 'fileSize - prefetchSize'.
    if (bufferCovers(position)) {
      telemetry.recordMetric(Metric.FOOTER_PREFETCH_HIT, 1L, Collections.emptyMap());
      return serveFromBuffer(position, dst);
    }

    if (prefetchSize <= 0 || position < fileSize - prefetchSize) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    AtomicBoolean isMiss = new AtomicBoolean(false);
    ByteBuffer footer =
        cacheManager.getFooter(
            gcsItemId,
            itemId -> {
              isMiss.set(true);
              return loadFooter(source);
            });
    adoptBuffer(footer, fileSize - footer.limit());

    if (!isMiss.get()) {
      telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());
    }

    return serveFromBuffer(position, dst);
  }

  /**
   * Serves a read issued before the object size is known.
   *
   * <p>Columnar readers open a file by reading its tail (footer length, magic, postscript), so the
   * first read is assumed to be close to the end of the object. A single window ending at the
   * requested range and extending {@link #estimatePrefetchSize()} bytes before it is fetched. That
   * one request both (a) makes the SDK channel expose the object metadata, which {@link
   * #resolveFileSizeFromItemInfo} then picks up without any additional metadata call, and (b) in
   * the common case already contains the footer, which is published to the cache.
   *
   * <p>If the guess is wrong (the window does not reach the end of the object), the bytes are still
   * kept for this stream so that nothing fetched is wasted, and the optimizer behaves like a normal
   * footer cache from then on. At most one such speculative read is ever issued per stream.
   */
  private int readWithUnknownFileSize(
      long position, ByteBuffer dst, VectoredSeekableByteChannel source) throws IOException {
    if (bufferCovers(position)) {
      telemetry.recordMetric(Metric.FOOTER_PREFETCH_HIT, 1L, Collections.emptyMap());
      return serveFromBuffer(position, dst);
    }

    long estimatedPrefetchSize = estimatePrefetchSize();
    if (!isLikelyFooterRead(position, dst, estimatedPrefetchSize)) {
      return 0;
    }

    // Set before issuing the request so that a failed or empty attempt is never repeated.
    speculativeReadAttempted = true;
    long startPosition = Math.max(0, position - estimatedPrefetchSize);
    long windowSize = position - startPosition + dst.remaining();
    ByteBuffer window = readWindow(startPosition, windowSize, source);
    resolveFileSizeFromItemInfo(source);
    if (window == null) {
      return 0;
    }

    ByteBuffer canonicalFooter = toCanonicalFooter(startPosition, window);
    if (canonicalFooter == null) {
      // Either not the tail of the object or the size is still unknown: keep the window for this
      // stream only.
      adoptBuffer(window, startPosition);
      return serveFromBuffer(position, dst);
    }

    AtomicBoolean isMiss = new AtomicBoolean(false);
    ByteBuffer footer =
        cacheManager.getFooter(
            gcsItemId,
            itemId -> {
              isMiss.set(true);
              telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
              return canonicalFooter;
            });
    adoptBuffer(footer, fileSize - footer.limit());

    if (!isMiss.get()) {
      telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());
    }

    return serveFromBuffer(position, dst);
  }

  private void adoptBuffer(ByteBuffer buffer, long startPosition) {
    localFooterBuffer = buffer;
    localBufferStartPosition = startPosition;
  }

  /**
   * Returns a copy of the canonical footer range {@code [fileSize - prefetchSize, fileSize)} taken
   * from {@code window}, or {@code null} if the size is unknown or the window does not cover it.
   */
  @Nullable
  private ByteBuffer toCanonicalFooter(long startPosition, ByteBuffer window) {
    if (fileSize == -1 || prefetchSize <= 0) {
      return null;
    }
    long canonicalStartPosition = fileSize - prefetchSize;
    if (startPosition > canonicalStartPosition || startPosition + window.limit() != fileSize) {
      return null;
    }

    ByteBuffer footerView = window.duplicate();
    footerView.position(Math.toIntExact(canonicalStartPosition - startPosition));
    ByteBuffer canonicalFooter = ByteBuffer.allocate(Math.toIntExact(prefetchSize));
    canonicalFooter.put(footerView);
    canonicalFooter.flip();

    return canonicalFooter;
  }

  /**
   * Reads up to {@code windowSize} bytes starting at {@code startPosition}, stopping early at the
   * end of the object. Returns {@code null} if nothing could be read.
   */
  @Nullable
  private ByteBuffer readWindow(
      long startPosition, long windowSize, VectoredSeekableByteChannel source) throws IOException {
    ByteBuffer window = ByteBuffer.allocate(Math.toIntExact(windowSize));
    long originalPosition = source.position();
    try {
      source.position(startPosition);
      while (window.hasRemaining()) {
        if (source.read(window) <= 0) {
          break;
        }
      }
    } finally {
      source.position(originalPosition);
    }
    window.flip();

    return window.limit() == 0 ? null : window;
  }

  /**
   * Heuristic used before the size is known. Empty requests and reads at offset 0 (sequential
   * scans) are passed through, as are requests larger than the footer prefetch size (data reads);
   * anything else is treated as a footer access.
   */
  private boolean isLikelyFooterRead(long position, ByteBuffer dst, long estimatedPrefetchSize) {
    return readOptions.isFooterPrefetchEnabled()
        && dst.hasRemaining()
        && position > 0
        && dst.remaining() <= estimatedPrefetchSize;
  }

  private long estimatePrefetchSize() {
    return Math.min(
        Math.max(
            readOptions.getFooterPrefetchSizeLargeFile(),
            readOptions.getFooterPrefetchSizeSmallFile()),
        MAX_SPECULATIVE_PREFETCH_SIZE);
  }

  /**
   * Last resort when the channel never exposed the metadata: {@link
   * VectoredSeekableByteChannel#size()} may issue a metadata request. Called at most once.
   */
  private void resolveFileSizeExplicitly(VectoredSeekableByteChannel source) throws IOException {
    long size = source.size();
    resolveFileSizeFromItemInfo(source);
    if (fileSize == -1) {
      fileSize = size;
      prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    }
  }

  private boolean bufferCovers(long position) {
    if (localFooterBuffer == null) {
      return false;
    }
    long offset = position - localBufferStartPosition;

    return offset >= 0 && offset < localFooterBuffer.limit();
  }

  private int serveFromBuffer(long position, ByteBuffer dst) {
    if (!bufferCovers(position)) {
      return 0;
    }
    ByteBuffer footerView = localFooterBuffer.duplicate();
    footerView.position(Math.toIntExact(position - localBufferStartPosition));

    int bytesToRead = Math.min(dst.remaining(), footerView.remaining());
    footerView.limit(footerView.position() + bytesToRead);
    dst.put(footerView);
    return bytesToRead;
  }

  private ByteBuffer loadFooter(VectoredSeekableByteChannel source) throws IOException {
    telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
    long startPosition = fileSize - prefetchSize;
    int bufferSize = Math.toIntExact(prefetchSize);
    ByteBuffer cacheBuffer = ByteBuffer.allocate(bufferSize);
    long originalPosition = source.position();
    try {
      source.position(startPosition);
      while (cacheBuffer.hasRemaining()) {
        if (source.read(cacheBuffer) == -1) {
          throw new IOException("Unexpected EOF encountered while reading footer.");
        }
      }
      cacheBuffer.flip();
      return cacheBuffer;
    } finally {
      source.position(originalPosition);
    }
  }

  private void resolveFileSizeFromItemInfo(VectoredSeekableByteChannel source) {
    GcsItemInfo itemInfo = source.getItemInfo();
    if (itemInfo == null || itemInfo.getSize() < 0) {
      return;
    }
    fileSize = itemInfo.getSize();
    prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    if (itemInfo.getItemId().getContentGeneration().isPresent()) {
      this.gcsItemId = itemInfo.getItemId();
    }
  }

  private static long calculatePrefetchSize(long fileSize, GcsReadOptions readOptions) {
    if (!readOptions.isFooterPrefetchEnabled()) {
      return 0;
    }
    return fileSize > LARGE_FILE_SIZE_THRESHOLD
        ? Math.min(readOptions.getFooterPrefetchSizeLargeFile(), fileSize)
        : Math.min(readOptions.getFooterPrefetchSizeSmallFile(), fileSize);
  }
}
