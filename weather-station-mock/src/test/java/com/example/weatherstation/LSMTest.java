package com.example.weatherstation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
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
  public void canPutAndGetData() {
    String str = "value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3value3";
    LSM<String, String> lsm = LSM.<String, String>builder().dataFolderPath(
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
  public void generatesNewSegment() {
    resetDataFolder();
    LSM<String, String> lsm = LSM.<String, String>builder().dataFolderPath(path)
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

  private void resetDataFolder() {
    File dataFolder = new File(System.getProperty("user.dir").concat("/src/test/java/com/example/weatherstation/data"));
    for (File file : dataFolder.listFiles()) {
      file.delete();
    }
  }

  @Test
  public void compactionGeneratesNewSegmentsAndUpdateKeyDirCorrectly() {
    resetDataFolder();
    LSM<String, String> lsm = LSM.<String, String>builder().dataFolderPath(path)
        .build();

    final int N = 4 * lsm.getSegmentSizeThreshold() * 1024 / 32 + 10;
    for (int i = 0; i < N; i++) {
      lsm.put("key" + (i % 1000), "value" + (i % 1000));
    }

    /// assert that a new segment is created with a file name equal to 1
    File newSegment = new File(lsm.getDataFolderPath() + "/1.1");
    newSegment = new File(lsm.getDataFolderPath() + "/2.1");
    lsm.compact();

    /// assert that a new segment is created with a file name equal to 1
    newSegment = new File(lsm.getDataFolderPath() + "/1.2");
    assertEquals(true, newSegment.exists());
    newSegment = new File(lsm.getDataFolderPath() + "/2.2");
    assertEquals(true, newSegment.exists());

    // assert all the values
    for (int i = 0; i < N; i++) {
      assertEquals("value" + (i % 1000), lsm.get("key" + (i % 1000)));
    }

    // assert that the key dir is updated correctly
    ConcurrentHashMap<String, ValueLocation> keyDir = lsm.getKeyDir();
    assertEquals(1000, keyDir.size());

  }

  @Test
  public void purgingRemovesOldSegments() {
    resetDataFolder();
    LSM<String, String> lsm = LSM.<String, String>builder().dataFolderPath(path).delayBetweenCompactionAndPurgingMS(0)
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
      Thread.sleep(100);
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
}
