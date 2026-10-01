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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeGcsClientImpl;
import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsFooterOptimizerTest {

  private static final long MB = 1024L * 1024;
  private static final long GB = 1024L * MB;

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("test.parquet").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(1000).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.parquet"))
          .setAttributes(ImmutableMap.of())
          .build();

  private GcsReadOptions readOptions;
  private Telemetry telemetry;
  private AnalyticsCacheManager mockCacheManager;
  private VectoredSeekableByteChannel realSource;
  private GcsFooterOptimizer optimizer;
  private byte[] testData;

  @BeforeEach
  void initializeOptimizerAndFakeStorage() throws IOException {
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(100)
            .setFooterPrefetchSizeLargeFile(500)
            .build();

    telemetry = spy(new Telemetry(ImmutableList.of()));
    mockCacheManager = mock(AnalyticsCacheManager.class);
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);

    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setGcsReadOptions(readOptions).build();
    GcsFileSystemOptions fileSystemOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(clientOptions)
            .setGcsCacheOptions(GcsCacheOptions.builder().build())
            .build();
    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);

    testData = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      testData[i] = (byte) (i % 256);
    }
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        testData);

    realSource = fakeFileSystem.open(FILE_INFO, readOptions);
  }

  @Test
  void isApplicable_footerPrefetchEnabled_returnsTrue() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
  }

  @Test
  void isApplicable_orcFile_returnsTrue() {
    GcsItemId orcItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.orc").build();
    assertThat(optimizer.isApplicable(orcItemId)).isTrue();
  }

  @Test
  void isApplicable_nonParquetFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    assertThat(optimizer.isApplicable(csvItemId)).isFalse();
  }

  @Test
  public void isApplicable_fileInfo_footerPrefetchEnabled_returnsTrue() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
  }

  @Test
  public void isApplicable_fileInfo_nonParquetFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    GcsItemInfo csvInfo = GcsItemInfo.builder().setItemId(csvItemId).setSize(1000).build();
    GcsFileInfo csvFileInfo = FILE_INFO.toBuilder().setItemInfo(csvInfo).build();
    assertThat(optimizer.isApplicable(csvFileInfo)).isFalse();
  }

  @Test
  public void isApplicable_fileInfo_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    assertThat(optimizer.isApplicable(FILE_INFO)).isFalse();
  }

  @Test
  void isApplicable_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_withFileInfo_initializesState() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void onOpen_withItemId_initializesState() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_footerHit_servesFromCache() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer cachedFooter = ByteBuffer.wrap(new byte[100]);
    cachedFooter.put(0, (byte) 42); // position 900
    cachedFooter.put(90, (byte) 99); // position 990
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()[0]).isEqualTo((byte) 99);
  }

  @Test
  void read_outsideFooterRange_returnsZero() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = optimizer.read(0, dst, realSource);
    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesReadEof = optimizer.read(1000, dst, realSource);

    assertThat(bytesReadEof).isEqualTo(-1);
    int bytesReadPastEof = optimizer.read(1010, dst, realSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_footerMiss_callsLoaderAndCaches() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    realSource.position(500L);
    // Let the loader actually execute to hit the real source
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()[0]).isEqualTo(testData[990]);
    assertThat(realSource.position()).isEqualTo(500L); // Position should be restored
  }

  @Test
  void read_loaderEncountersUnexpectedEof_throwsIOException() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    // Overwrite with a smaller file to force EOF during prefetch
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        new byte[50]);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst = ByteBuffer.allocate(10);

    assertThrows(IOException.class, () -> optimizer.read(990, dst, realSource));
  }

  @Test
  void read_largeFile_usesLargeFilePrefetchSize() throws IOException {
    long largeSize = 2 * GB;
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    // Have to create the file in Fake Storage or else lazy sizing might fail, but wait, lazy sizing
    // is only used if size is -1.
    // If the file is not really 2GB, reading from it will fail if it tries to read that far.
    // We only need to check the Cache Manager call!
    optimizer.onOpen(largeFile, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(500));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(largeSize - 500, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_lazyInitFileSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
  }

  @Test
  void read_footerPrefetchDisabled_returnsZeroAndDoesNotCache() throws IOException {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, realSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_largeFile_prefetchSizeIsCappedByFileSize() throws IOException {
    // Math.min check in calculatePrefetchSize
    long largeSize = 1400 * MB;
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeLargeFile((int) (1500 * MB))
            .build();
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();
    optimizer = new GcsFooterOptimizer(readOptions, telemetry);
    optimizer.onOpen(largeFile, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int unused = optimizer.read(0, dst, realSource);

    // Capped at the file size, the footer starts at offset 0, so even a read at the very start of
    // the object is inside the footer range and is served from the cache rather than passed
    // through. The buffer returned above is only a stand-in: a footer of the real prefetch size
    // cannot be allocated in a test.
    verify(mockCacheManager).getFooter(eq(ITEM_ID), any());
  }

  @Test
  void read_multipleReads_usesLocalBufferAndRecordsTelemetry() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    realSource.position(500L);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });
    ByteBuffer dst1 = ByteBuffer.allocate(10);
    ByteBuffer dst2 = ByteBuffer.allocate(10);

    int bytesRead1 = optimizer.read(990, dst1, realSource);
    int bytesRead2 = optimizer.read(980, dst2, realSource);

    assertThat(bytesRead1).isEqualTo(10);
    assertThat(dst1.array()[0]).isEqualTo(testData[990]);
    assertThat(bytesRead2).isEqualTo(10);
    assertThat(dst2.array()[0]).isEqualTo(testData[980]);
    verify(mockCacheManager, times(1)).getFooter(eq(ITEM_ID), any());
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_CACHE_MISS), eq(1L), any());
    verify(telemetry, times(1)).recordMetric(eq(Metric.FOOTER_PREFETCH_HIT), eq(1L), any());
  }

  @Test
  void read_fileSizeUninitialized_sourceGetItemInfoReturnsNull_returnsZero() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.getItemInfo()).thenReturn(null);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_fileSizeUninitialized_usesChannelItemInfo_doesNotCallSize() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.getItemInfo()).thenReturn(ITEM_INFO);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(ByteBuffer.allocate(100));
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    // Resolving the size must not go through the channel, which could issue a metadata request.
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_fileSizeUnknown_speculativeFooterRead_prefetchesFooterAndResolvesSizeWithoutSizeCall()
      throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(992, dst, mockSource);

    assertThat(bytesRead).isEqualTo(8);
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(testData, 992, 1000));
    // A single range read both prefetches the footer and resolves the object metadata.
    assertThat(dataReadCount.get()).isEqualTo(1);
    verify(mockSource, times(0)).size();
    // The optimizer must leave the position of the channel untouched.
    assertThat(mockSource.position()).isEqualTo(0);

    // The buffer is anchored at 'fileSize - prefetchSize', so the rest of the footer is served
    // from it rather than from another request.
    ByteBuffer footerStartDst = ByteBuffer.allocate(4);
    int footerStartBytesRead = optimizer.read(900, footerStartDst, mockSource);

    assertThat(footerStartBytesRead).isEqualTo(4);
    assertThat(footerStartDst.array()).isEqualTo(Arrays.copyOfRange(testData, 900, 904));
    assertThat(dataReadCount.get()).isEqualTo(1);
  }

  @Test
  void read_fileSizeUnknown_readTooLargeForFooter_passesThroughToDelegate() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    // Larger than the configured footer prefetch size, so it isn't a footer access.
    ByteBuffer dst = ByteBuffer.allocate(600);

    int bytesRead = optimizer.read(200, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_fileSizeUnknown_readAtStartOfObject_passesThroughToDelegate() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = lazyMetadataChannel(dataReadCount);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
    assertThat(dataReadCount.get()).isEqualTo(0);
    verify(mockSource, times(0)).size();
  }

  @Test
  void read_metadataNeverResolves_fallsBackToChannelSize_andStopsSpeculating() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    // A channel that never exposes the metadata, as an SDK version that the extractor doesn't
    // understand would behave.
    VectoredSeekableByteChannel mockSource =
        fakeChannel(testData, dataReadCount, /* itemInfo= */ null);
    when(mockSource.size()).thenReturn((long) testData.length);

    int firstBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);
    int secondBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);
    int thirdBytesRead = optimizer.read(992, ByteBuffer.allocate(8), mockSource);

    assertThat(firstBytesRead).isEqualTo(8);
    assertThat(secondBytesRead).isEqualTo(8);
    assertThat(thirdBytesRead).isEqualTo(8);
    // A single inconclusive speculative read is enough: the size is then resolved explicitly, once,
    // instead of speculating on every subsequent read.
    verify(mockSource, times(1)).size();
  }

  @Test
  void read_speculativeWindowNotAtEndOfObject_laterFooterReadIsNotShort() throws IOException {
    byte[] largeData = new byte[5000];
    for (int i = 0; i < largeData.length; i++) {
      largeData[i] = (byte) (i % 256);
    }
    GcsItemInfo largeItemInfo =
        GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeData.length).build();
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockCacheManager.getFooter(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1, AnalyticsCacheManager.FooterLoader.class).load(ITEM_ID));
    AtomicInteger dataReadCount = new AtomicInteger(0);
    VectoredSeekableByteChannel mockSource = fakeChannel(largeData, dataReadCount, largeItemInfo);
    // Speculating around 4450 reads [3950, 4950): it overlaps the footer (which starts at 4900)
    // without covering all of it.
    int unused = optimizer.read(4450, ByteBuffer.allocate(8), mockSource);

    // A footer read that extends past 4950 must not be served as a short 50-byte read.
    ByteBuffer dst = ByteBuffer.allocate(100);
    int bytesRead = optimizer.read(4900, dst, mockSource);

    assertThat(bytesRead).isEqualTo(100);
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(largeData, 4900, 5000));
  }

  @Test
  void read_cachedFooterShorterThanPrefetchSize_servesFromTheCorrectOffset() throws IOException {
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    // A footer cached by a stream configured with a smaller prefetch size: the buffer covers
    // [fileSize - 40, fileSize), not [fileSize - prefetchSize, fileSize).
    ByteBuffer cachedFooter = ByteBuffer.wrap(Arrays.copyOfRange(testData, 960, 1000));
    when(mockCacheManager.getFooter(eq(ITEM_ID), any())).thenReturn(cachedFooter);
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    ByteBuffer dst = ByteBuffer.allocate(8);

    int bytesRead = optimizer.read(992, dst, mockSource);

    assertThat(bytesRead).isEqualTo(8);
    // Anchoring on the configured prefetch size instead of the buffer length would silently
    // return the bytes of a different offset.
    assertThat(dst.array()).isEqualTo(Arrays.copyOfRange(testData, 992, 1000));
  }

  /**
   * Returns a channel over {@link #testData} that, like the SDK one, only exposes the object
   * metadata once a read response came back.
   */
  private VectoredSeekableByteChannel lazyMetadataChannel(AtomicInteger dataReadCount)
      throws IOException {
    return fakeChannel(testData, dataReadCount, ITEM_INFO);
  }

  /**
   * Returns a channel over {@code data} that exposes {@code itemInfo} once a read response came
   * back, or never when {@code itemInfo} is {@code null}.
   */
  private VectoredSeekableByteChannel fakeChannel(
      byte[] data, AtomicInteger dataReadCount, GcsItemInfo itemInfo) throws IOException {
    long[] channelPosition = {0};
    VectoredSeekableByteChannel mockSource = mock(VectoredSeekableByteChannel.class);
    when(mockSource.position()).thenAnswer(invocation -> channelPosition[0]);
    when(mockSource.position(anyLong()))
        .thenAnswer(
            invocation -> {
              channelPosition[0] = invocation.getArgument(0);
              return mockSource;
            });
    when(mockSource.getItemInfo())
        .thenAnswer(invocation -> dataReadCount.get() > 0 ? itemInfo : null);
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer dst = invocation.getArgument(0);
              int startPosition = (int) channelPosition[0];
              int length = Math.min(dst.remaining(), data.length - startPosition);
              if (length <= 0) {
                return -1;
              }
              dataReadCount.incrementAndGet();
              dst.put(data, startPosition, length);
              channelPosition[0] = startPosition + length;
              return length;
            });

    return mockSource;
  }
}
