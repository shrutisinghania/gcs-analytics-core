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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsBidiReadChannel extends GcsReadChannel {
  private static final Logger logger = LoggerFactory.getLogger(GcsBidiReadChannel.class);
  static final int DEFAULT_READ_BUFFER_CHUNK_SIZE = 2 * 1024 * 1024;

  /**
   * Immutable heap-backed look-ahead chunk. Copying {@link DisposableByteString} bytes into a plain
   * {@code byte[]} releases Netty's pooled direct buffer immediately and allows lock-free reads
   * across both {@link #read(ByteBuffer)} and concurrent {@link #read(long, ByteBuffer)} calls.
   */
  private static final class BufferChunk {
    final long startOffset;
    final byte[] data;

    BufferChunk(long startOffset, byte[] data) {
      this.startOffset = startOffset;
      this.data = data;
    }

    boolean contains(long offset) {
      return offset >= startOffset && offset < startOffset + data.length;
    }

    int copyTo(long offset, ByteBuffer dst) {
      int relOffset = (int) (offset - startOffset);
      int available = data.length - relOffset;
      int bytesToCopy = Math.min(dst.remaining(), available);
      if (bytesToCopy <= 0) {
        return 0;
      }
      dst.put(data, relOffset, bytesToCopy);
      return bytesToCopy;
    }
  }

  private final long bidiClientTimeoutSeconds;
  private final int readBufferChunkSize;
  private final BlobId blobId;
  @Nullable private final BidiSessionPool sessionPool;
  @Nullable private final BidiSessionPool.PooledSession pooledSession;
  private final ApiFuture<BlobReadSession> sessionFuture;
  private volatile BlobReadSession blobReadSession;
  private volatile BufferChunk internalChunk;

  private final AtomicInteger pendingReads = new AtomicInteger(0);
  private final AtomicBoolean closeInitiated = new AtomicBoolean(false);
  private final AtomicBoolean sessionReleased = new AtomicBoolean(false);
  private volatile boolean sessionFailed = false;
  private volatile boolean closed = false;
  private volatile long position = 0;

  private volatile long objectSize = -1;
  private volatile boolean metadataInitialized = false;

  GcsBidiReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, itemInfo, readOptions, executorServiceSupplier, telemetry, null);
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      @Nullable BidiSessionPool sessionPool)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
    this.bidiClientTimeoutSeconds = readOptions.getBidiTimeout();
    this.readBufferChunkSize = readOptions.getChunkSize().orElse(DEFAULT_READ_BUFFER_CHUNK_SIZE);
    this.blobId = initBlobId();
    this.sessionPool = sessionPool;
    if (sessionPool != null) {
      this.pooledSession = sessionPool.acquire(storage, blobId);
      this.sessionFuture = this.pooledSession.getSessionFuture();
      this.blobReadSession = this.pooledSession.getResolvedSession();
    } else {
      this.pooledSession = null;
      this.sessionFuture = storage.blobReadSession(blobId);
    }
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, itemId, readOptions, executorServiceSupplier, telemetry, null, null);
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      @Nullable ItemInfoProvider itemInfoProvider)
      throws IOException {
    this(storage, itemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider, null);
  }

  GcsBidiReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      @Nullable ItemInfoProvider itemInfoProvider,
      @Nullable BidiSessionPool sessionPool)
      throws IOException {
    super(storage, itemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    this.bidiClientTimeoutSeconds = readOptions.getBidiTimeout();
    this.readBufferChunkSize = readOptions.getChunkSize().orElse(DEFAULT_READ_BUFFER_CHUNK_SIZE);
    this.blobId = initBlobId();
    this.sessionPool = sessionPool;
    if (sessionPool != null) {
      this.pooledSession = sessionPool.acquire(storage, blobId);
      this.sessionFuture = this.pooledSession.getSessionFuture();
      this.blobReadSession = this.pooledSession.getResolvedSession();
    } else {
      this.pooledSession = null;
      this.sessionFuture = storage.blobReadSession(blobId);
    }
  }

  @Override
  protected ReadStrategy createReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions readOptions, GcsItemInfo itemInfo) {
    return new ReadStrategy() {
      @Override
      public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) {
        throw new UnsupportedOperationException(
            "Standard read is not supported on Bidi channel yet.");
      }

      @Override
      public void position(long newPosition) {}

      @Override
      public long getLimit() {
        return 0;
      }

      @Override
      public boolean isEof(long position) {
        return true;
      }

      /**
       * Bidi sessions have no SDK {@link ReadChannel}; metadata comes from {@link
       * BlobReadSession#getBlobInfo()} in {@link #ensureMetadataInitialized()} instead.
       */
      @Override
      @Nullable
      public ReadChannel getSdkReadChannel() {
        return null;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public boolean isOpen() {
    return !closed && super.isOpen();
  }

  @Override
  public boolean isPositionedReadSupported() {
    return true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    pendingReads.incrementAndGet();
    try {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      if (!dst.hasRemaining()) {
        return 0;
      }
      long objSize = size();
      long currentPosition = this.position;
      if (currentPosition >= objSize) {
        return -1;
      }
      int bytesRead = readAtOffset(currentPosition, dst, objSize);
      if (bytesRead > 0) {
        this.position = currentPosition + bytesRead;
      }
      return bytesRead;
    } finally {
      decrementPendingReads(1);
    }
  }

  @Override
  public int read(long offset, ByteBuffer dst) throws IOException {
    pendingReads.incrementAndGet();
    try {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      if (offset < 0) {
        throw new EOFException(
            String.format(
                "Invalid seek offset: position value (%d) must be >= 0 for '%s'", offset, itemId));
      }
      if (!dst.hasRemaining()) {
        return 0;
      }
      long objSize = size();
      if (offset >= objSize) {
        return -1;
      }
      return readAtOffset(offset, dst, objSize);
    } finally {
      decrementPendingReads(1);
    }
  }

  private int readAtOffset(long offset, ByteBuffer dst, long objSize) throws IOException {
    BufferChunk chunk = this.internalChunk;
    if (chunk != null && chunk.contains(offset)) {
      return chunk.copyTo(offset, dst);
    }

    int requested = dst.remaining();
    if (readBufferChunkSize <= 0 || requested >= readBufferChunkSize) {
      long bytesToRequest = Math.min((long) requested, objSize - offset);
      return readBytesFromSession(dst, offset, bytesToRequest);
    }

    long chunkBytesToRequest = Math.min((long) readBufferChunkSize, objSize - offset);
    byte[] fetched = fetchBytesFromSession(offset, chunkBytesToRequest);
    if (fetched.length == 0) {
      return 0;
    }
    BufferChunk newChunk = new BufferChunk(offset, fetched);
    this.internalChunk = newChunk;
    return newChunk.copyTo(offset, dst);
  }

  @Override
  public long position() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (newPosition < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              newPosition, itemId));
    }
    this.position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    ensureMetadataInitialized();
    return objectSize;
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    if (closed) {
      ClosedChannelException e =
          new ClosedChannelException() {
            @Override
            public String getMessage() {
              return "Reader is closed.";
            }
          };
      for (GcsObjectRange range : ranges) {
        range.getByteBufferFuture().completeExceptionally(e);
      }
      throw e;
    }
    ImmutableList<GcsObjectRange> pendingRanges =
        VectoredIoUtil.completeDegenerateRanges(ranges, itemId);
    if (pendingRanges.isEmpty()) {
      return;
    }

    pendingReads.incrementAndGet();
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();
    ImmutableList<GcsObjectCombinedRange> combinedRanges;
    BlobReadSession session;
    try {
      if (closed) {
        ClosedChannelException e =
            new ClosedChannelException() {
              @Override
              public String getMessage() {
                return "Reader is closed.";
              }
            };
        for (GcsObjectRange range : pendingRanges) {
          range.getByteBufferFuture().completeExceptionally(e);
        }
        throw e;
      }
      session = getBlobReadSession();
      combinedRanges =
          VectoredIoUtil.mergeGcsObjectRanges(
              pendingRanges,
              vectoredReadOptions.getMaxMergeGap(),
              vectoredReadOptions.getMaxMergeSize());
      pendingReads.addAndGet(combinedRanges.size());
    } catch (IOException | RuntimeException e) {
      for (GcsObjectRange range : pendingRanges) {
        range.getByteBufferFuture().completeExceptionally(e);
      }
      throw e;
    } finally {
      decrementPendingReads(1);
    }

    for (GcsObjectCombinedRange combinedRange : combinedRanges) {
      try {
        readAndAttachCallback(combinedRange, session, allocate);
      } catch (RuntimeException e) {
        sessionFailed = true;
        completeWithException(combinedRange, e);
        decrementPendingReads(1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!closeInitiated.compareAndSet(false, true)) {
      return;
    }
    synchronized (this) {
      closed = true;
      internalChunk = null;
      super.close();
    }
    if (pendingReads.get() == 0) {
      releaseSessionOnce();
    }
  }

  private void decrementPendingReads(int count) {
    if (pendingReads.addAndGet(-count) == 0 && closeInitiated.get()) {
      releaseSessionOnce();
    }
  }

  private void releaseSessionOnce() {
    if (!sessionReleased.compareAndSet(false, true)) {
      return;
    }
    BlobReadSession session = blobReadSession;
    blobReadSession = null;
    if (sessionPool != null && pooledSession != null) {
      sessionPool.release(blobId, pooledSession, !sessionFailed);
    } else if (session != null) {
      try {
        session.close();
      } catch (IOException e) {
        logger.debug("Exception closing BlobReadSession for {}", blobId, e);
      }
    }
  }

  private BlobId initBlobId() {
    String bucketName = itemId.getBucketName();
    checkArgument(itemId.getObjectName().isPresent(), "ObjectName cannot be empty");
    String objectName = itemId.getObjectName().get();
    java.util.Optional<Long> generation =
        itemId.getContentGeneration().isPresent()
            ? itemId.getContentGeneration()
            : (itemInfo != null ? itemInfo.getContentGeneration() : java.util.Optional.empty());
    return generation
        .map(gen -> BlobId.of(bucketName, objectName, gen))
        .orElse(BlobId.of(bucketName, objectName));
  }

  private byte[] fetchBytesFromSession(long offset, long length) throws IOException {
    BlobReadSession session = getBlobReadSession();
    ApiFuture<DisposableByteString> futureBytes =
        session.readAs(
            ReadProjectionConfigs.asFutureByteString().withRangeSpec(RangeSpec.of(offset, length)));
    try (DisposableByteString dbs = futureBytes.get(bidiClientTimeoutSeconds, TimeUnit.SECONDS)) {
      ByteString byteString = dbs.byteString();
      return byteString.isEmpty() ? new byte[0] : byteString.toByteArray();
    } catch (InterruptedException e) {
      sessionFailed = true;
      futureBytes.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupted while reading from stream", e);
    } catch (ExecutionException e) {
      sessionFailed = true;
      Throwable cause = e.getCause();
      if (cause instanceof StorageException && ((StorageException) cause).getCode() == 404) {
        throw new FileNotFoundException("Object not found during read: " + blobId);
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException("Failed to read bytes from bidirectional session", e);
    } catch (TimeoutException e) {
      sessionFailed = true;
      futureBytes.cancel(true);
      throw new IOException("Timed out waiting for bytes from bidirectional stream", e);
    }
  }

  private int readBytesFromSession(ByteBuffer dst, long offset, long length) throws IOException {
    BlobReadSession session = getBlobReadSession();
    ApiFuture<DisposableByteString> futureBytes =
        session.readAs(
            ReadProjectionConfigs.asFutureByteString().withRangeSpec(RangeSpec.of(offset, length)));
    try (DisposableByteString dbs = futureBytes.get(bidiClientTimeoutSeconds, TimeUnit.SECONDS)) {
      ByteString byteString = dbs.byteString();
      int bytesRead = byteString.size();
      if (bytesRead > 0) {
        byteString.copyTo(dst);
      }
      return bytesRead;
    } catch (InterruptedException e) {
      sessionFailed = true;
      futureBytes.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupted while reading from stream", e);
    } catch (ExecutionException e) {
      sessionFailed = true;
      Throwable cause = e.getCause();
      if (cause instanceof StorageException && ((StorageException) cause).getCode() == 404) {
        throw new FileNotFoundException("Object not found during read: " + blobId);
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException("Failed to read bytes from bidirectional session", e);
    } catch (TimeoutException e) {
      sessionFailed = true;
      futureBytes.cancel(true);
      throw new IOException("Timed out waiting for bytes from bidirectional stream", e);
    }
  }

  private void ensureMetadataInitialized() throws IOException {
    if (metadataInitialized) {
      return;
    }
    synchronized (this) {
      if (metadataInitialized) {
        return;
      }
      BlobInfo blobInfo = null;
      IOException sessionFailure = null;
      try {
        BlobReadSession session = getBlobReadSession();
        blobInfo = (session != null) ? session.getBlobInfo() : null;
      } catch (FileNotFoundException e) {
        throw e;
      } catch (IOException e) {
        logger.debug("Falling back to item info lookup for {}", blobId, e);
        sessionFailure = e;
      }

      if (blobInfo == null || blobInfo.getSize() == null || blobInfo.getSize() < 0) {
        this.objectSize = resolveSizeFromItemInfo(sessionFailure);
      } else {
        this.objectSize = blobInfo.getSize();
        if (!hasResolvedSize(itemInfo)) {
          Long generation = blobInfo.getGeneration();
          updateGcsItemMetadata(
              new GcsReadChannelMetadataExtractor.ExtractedMetadata(
                  this.objectSize, generation != null ? generation : -1L));
        }
      }
      this.metadataInitialized = true;
    }
  }

  private long resolveSizeFromItemInfo(@Nullable IOException sessionFailure) throws IOException {
    try {
      long size = super.size();
      if (size < 0) {
        throw new IOException("Unable to resolve object size for " + blobId);
      }
      return size;
    } catch (IOException fallbackFailure) {
      if (sessionFailure != null) {
        fallbackFailure.addSuppressed(sessionFailure);
      }
      throw fallbackFailure;
    }
  }

  private BlobReadSession getBlobReadSession() throws IOException {
    if (blobReadSession != null) {
      return blobReadSession;
    }
    synchronized (this) {
      if (closed) {
        throw new IOException("Reader is closed.");
      }
      if (blobReadSession == null) {
        try {
          blobReadSession = this.sessionFuture.get(bidiClientTimeoutSeconds, TimeUnit.SECONDS);
          if (pooledSession != null && blobReadSession != null) {
            pooledSession.setResolvedSession(blobReadSession);
          }
        } catch (InterruptedException e) {
          sessionFailed = true;
          Thread.currentThread().interrupt();
          throw new IOException("Failed to get BlobReadSession due to thread interruption", e);
        } catch (ExecutionException e) {
          sessionFailed = true;
          Throwable cause = e.getCause();
          if (cause instanceof StorageException && ((StorageException) cause).getCode() == 404) {
            throw new FileNotFoundException("Object not found: " + blobId);
          }
          throw new IOException("Failed to get BlobReadSession", e);
        } catch (TimeoutException e) {
          sessionFailed = true;
          throw new IOException("Failed to get BlobReadSession due to client timeout limit", e);
        }
      }
    }
    return blobReadSession;
  }

  private void readAndAttachCallback(
      GcsObjectCombinedRange combinedRange,
      BlobReadSession session,
      IntFunction<ByteBuffer> allocate) {
    ApiFuture<DisposableByteString> futureBytes =
        session.readAs(
            ReadProjectionConfigs.asFutureByteString()
                .withRangeSpec(RangeSpec.of(combinedRange.getOffset(), combinedRange.getLength())));

    ApiFutures.addCallback(
        futureBytes,
        new ApiFutureCallback<DisposableByteString>() {
          @Override
          public void onFailure(Throwable t) {
            try {
              sessionFailed = true;
              completeWithException(combinedRange, t);
              logger.debug(
                  "Vectored Read failed for combined range starting from {} with length {} "
                      + "({} underlying ranges)",
                  combinedRange.getOffset(),
                  combinedRange.getLength(),
                  combinedRange.getUnderlyingRanges().size());
            } finally {
              decrementPendingReads(1);
            }
          }

          @Override
          public void onSuccess(DisposableByteString disposableByteString) {
            try {
              processBytesAndCompleteRanges(disposableByteString, combinedRange, allocate);
            } catch (Throwable t) {
              completeWithException(combinedRange, t);
            } finally {
              decrementPendingReads(1);
            }
          }
        },
        executorServiceSupplier.get());
  }

  private void processBytesAndCompleteRanges(
      DisposableByteString disposableByteString,
      GcsObjectCombinedRange combinedRange,
      IntFunction<ByteBuffer> allocate)
      throws IOException {
    try (DisposableByteString dbs = disposableByteString) {
      ByteString byteString = dbs.byteString();
      int bytesRead = byteString.size();
      if (bytesRead > combinedRange.getLength()) {
        throw new IOException(
            String.format(
                "Received more bytes (%d) than requested (%d) for combined range: %s, path: %s",
                bytesRead, combinedRange.getLength(), combinedRange, itemId));
      }
      ByteBuffer buf = allocate.apply(bytesRead);
      if (buf == null) {
        throw new NullPointerException("Allocator returned a null ByteBuffer!");
      }
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip();
      ImmutableList<GcsObjectRange> underlyingRanges = combinedRange.getUnderlyingRanges();
      if (underlyingRanges.size() == 1 && bytesRead >= combinedRange.getLength()) {
        underlyingRanges.get(0).getByteBufferFuture().complete(buf);
        return;
      }
      for (GcsObjectRange underlyingRange : underlyingRanges) {
        completeUnderlyingRange(combinedRange, underlyingRange, bytesRead, buf);
      }
    }
  }

  private void completeUnderlyingRange(
      GcsObjectCombinedRange combinedRange,
      GcsObjectRange underlyingRange,
      int bytesRead,
      ByteBuffer dataBuffer) {
    long maxPosition = combinedRange.getOffset() + bytesRead;
    long underlyingRangeEnd = underlyingRange.getOffset() + underlyingRange.getLength();
    if (underlyingRangeEnd <= maxPosition) {
      underlyingRange
          .getByteBufferFuture()
          .complete(
              VectoredIoUtil.fetchUnderlyingRangeData(dataBuffer, combinedRange, underlyingRange));
      return;
    }
    underlyingRange
        .getByteBufferFuture()
        .completeExceptionally(
            new EOFException(
                String.format(
                    "EOF reached before child range could be populated, childRange: %s, "
                        + "combinedRange: %s, expected length: %d, readBytes: %d, path: %s",
                    underlyingRange, combinedRange, combinedRange.getLength(), bytesRead, itemId)));
  }

  private static void completeWithException(GcsObjectCombinedRange combinedRange, Throwable t) {
    for (GcsObjectRange underlyingRange : combinedRange.getUnderlyingRanges()) {
      underlyingRange.getByteBufferFuture().completeExceptionally(t);
    }
  }
}
