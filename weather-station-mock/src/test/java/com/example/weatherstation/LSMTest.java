package com.example.weatherstation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.*;

public class LSMTest {

  String path = System.getProperty("user.dir").concat("/src/test/java/com/example/weatherstation/data");

  // before all tests create the data folder
  @BeforeClass
  public static void createDataFolder() {
    File dataFolder = new File(System.getProperty("user.dir").concat("/src/test/java/com/example/weatherstation/data"));
    dataFolder.mkdir();
  }

  @Test
  public void test1() {
    System.out.println(path);
    Assert.assertEquals(1, 1);
  }

  @Test
  public void canPutAndGetData() throws ClassNotFoundException, IOException {
    String str = "value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3";
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(
        path)
        .build();
    lsm.put("key1", "value1");
    lsm.put("key2", null);
    lsm.put("key3",
        str);

    assertEquals(null, "value1", lsm.get("key1"));
    assertEquals(null, null, lsm.get("key2"));
    assertEquals(null, str, lsm.get("key3"));
  }

  @Test
  public void generatesNewSegment() throws ClassNotFoundException, IOException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path)
        .build();
    for (int i = 0; i < 2 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10; i++) {
      lsm.put("key1" + i, "value1" + i); // around 11 + 13 + 4 * 2 = 32 bytes
    }

    /// assert that a new segment is created with a file name equal to 1
    File newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    assertEquals(true, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    assertEquals(true, newSegment.exists());
  }

  private static void resetDataFolder() {
    File dataFolder = new File(System.getProperty("user.dir").concat("/src/test/java/com/example/weatherstation/data"));
    for (File file : dataFolder.listFiles()) {
      file.delete();
    }
  }

  @Test
  public void compactionGeneratesNewSegmentsAndUpdateKeyDirCorrectly() throws ClassNotFoundException, IOException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 10 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 1000), "value" + (i % 1000));
    }

    /// assert that a new segment is created with a file name equal to 1
    lsm.compact();

    // File newSegment;
    // /// assert that a new segment is created with a file name equal to 1
    // newSegment = new File(lsm.getDataFolderPath() + "/1.2");
    // assertEquals(true, newSegment.exists());
    // newSegment = new File(lsm.getDataFolderPath() + "/2.2");
    // assertEquals(true, newSegment.exists());

    // // assert all the values
    // for (int i = 0; i < N; i++) {
    // assertEquals("value" + (i % 1000), lsm.get("key" + (i % 1000)));
    // }

    // // assert that the key dir is updated correctly
    // ConcurrentHashMap<String, ValueLocation> keyDir = lsm.getKeyDir();
    // assertEquals(1000, keyDir.size());

  }

  @Test
  public void purgingRemovesOldSegments() throws ClassNotFoundException, IOException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path)
        .delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 4 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 1000), "value" + (i % 1000));
    }

    /// assert that a new segment is created with a file name equal to 1
    File newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    lsm.compact();

    // sleep for 100 ms
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    /// assert that a new segment is created with a file name equal to 1
    newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    assertEquals(false, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    assertEquals(false, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/3.1");
    assertEquals(false, newSegment.exists());

  }

  @Test
  public void purgingDoesNotRemovesOldSegmentsInTime() throws ClassNotFoundException, IOException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path)
        .delayBetweenCompactionAndPurgingMS(10000)
        .build();

    final int N = 4 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 10), "value" + (i % 10));
    }

    /// assert that a new segment is created with a file name equal to 1
    File newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    lsm.compact();

    // sleep for 100 ms
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    /// assert that a new segment is created with a file name equal to 1
    newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    assertEquals(true, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    assertEquals(true, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/3.1");
    assertEquals(true, newSegment.exists());

  }

  @Test
  public void canRecoverKeyDir() throws ClassNotFoundException, IOException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 3 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 14;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 500000), "value" + (i % 500000));
    }
    // get keyDir
    ConcurrentHashMap<String, ValueLocation> oldKeyDir = lsm.getKeyDir();

    ConcurrentHashMap<String, ValueLocation> newKeyDir = lsm.generateKeyDirFromDisk();

    // compare the two keyDirs
    assertEquals(oldKeyDir.size(), newKeyDir.size());
    for (String key : oldKeyDir.keySet()) {
      assertEquals(oldKeyDir.get(key).getFile(), newKeyDir.get(key).getFile());
      assertEquals(oldKeyDir.get(key).getOffset(), newKeyDir.get(key).getOffset());
    }
  }

  @Test
  public void canRecoverKeyDirAfterCompaction() throws ClassNotFoundException, IOException, InterruptedException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 3 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 14;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 500000), "value" + (i % 500000));
    }
    // compact
    lsm.compact();

    Thread.sleep(10);

    // get keyDir
    ConcurrentHashMap<String, ValueLocation> oldKeyDir = lsm.getKeyDir();

    ConcurrentHashMap<String, ValueLocation> newKeyDir = lsm.generateKeyDirFromDisk();

    // compare the two keyDirs
    assertEquals(oldKeyDir.size(), newKeyDir.size());
    for (String key : oldKeyDir.keySet()) {
      assertEquals(oldKeyDir.get(key).getFile(), newKeyDir.get(key).getFile());
      assertEquals(oldKeyDir.get(key).getOffset(), newKeyDir.get(key).getOffset());
    }
  }

  @Test
  public void canRecoverLSM() throws ClassNotFoundException, IOException, InterruptedException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 10 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 1000), "value" + (i % 1000));
    }

    lsm = null;

    LSM<String, String> lsm2 = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(10000)
        .build();
    // assert all the values
    for (int i = 0; i < N; i++) {
      assertEquals("value" + (i % 1000), lsm2.get("key" + (i % 1000)));
    }

    // assert that the key dir is updated correctly
    ConcurrentHashMap<String, ValueLocation> keyDir = lsm2.getKeyDir();
    assertEquals(1000, keyDir.size());
  }

  @Test
  public void canRecoverLSMAfterCompaction() throws ClassNotFoundException, IOException, InterruptedException {
    resetDataFolder();
    LSM<String, String> lsm = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();

    final int N = 10 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 1000), "value" + (i % 1000));
    }

    lsm.compact();

    Thread.sleep(1000);
    lsm = null;

    LSM<String, String> lsm2 = new LSM.Builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
        .build();
    // assert all the values
    for (int i = 0; i < N; i++) {
      assertEquals("value" + (i % 1000), lsm2.get("key" + (i % 1000)));
    }

    // assert that the key dir is updated correctly
    ConcurrentHashMap<String, ValueLocation> keyDir = lsm2.getKeyDir();
    assertEquals(1000, keyDir.size());

    Integer latestId = getLatestSegmentId(path);

    for (int i = 0; i < N; i++) {
      lsm2.put("key" + (i % 1000), "value" + (i % 1000));
    }

    File newSegment = new File(lsm2.getDataFolderPath() + File.separator + latestId.toString() + ".1");
    assertTrue(newSegment.exists());

    latestId++;
    newSegment = new File(lsm2.getDataFolderPath() + File.separator + latestId.toString() + ".1");
    assertTrue(newSegment.exists());

    latestId++;
    newSegment = new File(lsm2.getDataFolderPath() + File.separator + latestId.toString() + ".1");
    assertTrue(newSegment.exists());

  }

  private static int getLatestSegmentId(String folderPath) {
    File file = new File(folderPath);
    if (file.exists()) {
      List<File> files = Arrays.asList(file.listFiles());
      return files.stream().map(f -> f.getName().split("\\.")[0])
          .distinct().mapToInt(Integer::valueOf).max().orElse(0);

    } else {
      file.mkdirs();
      return 0;
    }
  }

  @AfterClass
  public static void cleanUp() {
    resetDataFolder();
    File dataFolder = new
    File(System.getProperty("user.dir").concat("/src/test/java/com/example/weatherstation/data"));
    dataFolder.delete();
  }
}
