package com.example.weatherstation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Builder;

@Builder(builderMethodName = "builder")
public class LSM<K, V> {
  public static void main(String[] args) {
    // in memory HashMap <Key(type1), file and offset(Long)>

    /*
     * constructor (segment_size) <Type1,Type2>
     * init a concurrent hashtable
     * 
     * put (key(type1), value(type2) ) {
     * if file size is above threshold create a new file
     * otherwise append to the current file
     * 
     * add to the hashmap key = file + offset
     * add a record to the log file
     * record is written as `${key}:${value}\n`
     * }
     * 
     * get() {
     * 
     * get file and offset from hashmap and start reading file data
     * start reading key until `:`
     * start reading value until `\n`
     * return value
     * 
     * on failure redo write (for 2 times max) (needed because of compaction)
     * // on compaction:
     * 1. T1 updates disk (for a certain key value)
     * 2. T2 reads hashmap with old offset
     * 3. T1 deletes the segment containing the old offset
     * 4- t2 reads from disk with old (and deleted offset)
     * 
     * soln:
     * - try reading again
     * - or say you offer eventual consistency
     * - or schedule old segments deletion late (a day after for example, would
     * require keeping meta data about the segment expiry date)
     * 
     * }
     * 
     * 
     * compact(string filePath) {
     * create a new file with a new id
     * 
     * // problem: (conflict between writer and compaction)
     * 1. T1 (writer thread) updates disk
     * 2. T2 (compaction thread) updates disk
     * 3. T1 updates map
     * 4. T2 updates map (Wrong expected t1 offset but got key 2 offset)
     * 
     * soln:
     * When updating map T2 must verify the map has the offset he is deleting (use a
     * concurrent hashmap)
     * 
     * // problem: (conflict between compaction and reader)
     * 1. T1 compacts disk
     * 2. T2 reads keyDir
     * 3. T1 updates keyDir
     * 4. T2 reads wrong file offset (update by T1)
     * 
     * soln:
     * - use locks or semaphores
     * - delete old segments later in the day .
     * 
     * 
     * }
     * 
     * 
     * For example. after the compaction thread (C_thread) compacted the segment. A
     * reader thread could read from the in-memory hashmap to get the offset then
     * sleep for 10 days. after it wakes up, it will have the old offset (the before
     * compaction offset) and fail. (this could be solved by retrying)
     * 
     * Another issue is having the compaction thread and the writer thread write to
     * the same in-memory hashmap simultaneously. Even if we use something like a
     * concurrent hashmap we would still need a semaphore or a lock on the hashmap.,
     * because the Compaction thread could override the writer thread's latest value
     * after compaction
     */

  }

  /*
   * Initial version of the LSM segment
   */
  @Builder.Default
  private String initialVersion = ".1";

  /*
   * Segment size threshold in kilo bytes
   */
  @Builder.Default
  private int segmentSizeThreshold = 10;

  /*
   * Path of the folder that contains the segments
   */
  // TODO: use OS seperator
  @Builder.Default
  private String dataFolderPath = System.getProperty("user.dir").concat("/data");

  /*
   * Id of the last segment file being written to. (auto incrementing id)
   */
  @Builder.Default
  private long activeSegmentId = 0;

  @Builder.Default
  private final int delayBetweenCompactionAndPurgingMS = 3600000;

  @Builder.Default
  private ConcurrentHashMap<K, ValueLocation> keyDir = new ConcurrentHashMap<>();

  /*
   * 
   * TODO: handle reading wrong keyDir offset (due to compaction) by retrying
   * once.
   */
  // TODO: handle ClassNotFoundException
  public V get(K key) {
    ValueLocation valueLocation = keyDir.get(key);
    if (valueLocation == null) {
      return null;
    }
    File file = new File(dataFolderPath, valueLocation.getFile());
    try {
      return readRecord(file, valueLocation.getOffset()).getValue();
    } catch (ClassNotFoundException | IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private Tuple<K, V> readRecord(File file, int offset) throws IOException, ClassNotFoundException {
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      fileInputStream.skip(offset);
      BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
      return readRecord(bufferedInputStream, offset);
    }
  }

  private Tuple<K, V> readRecord(InputStream InputStream, int offset) throws IOException, ClassNotFoundException {
    DataInputStream dataInputStream = new DataInputStream(InputStream);

    // TODO: remove
    int keyLength = dataInputStream.readInt();
    byte[] keyBytes = new byte[keyLength];
    InputStream.read(keyBytes);
    K key = (K) convertFromByteArray(keyBytes);

    int valueLength = dataInputStream.readInt();
    byte[] valueBytes = new byte[valueLength];
    InputStream.read(valueBytes);
    V value = (V) convertFromByteArray(valueBytes);

    // get the offset of the next record

    // TODO: abstract this
    int recordLength = keyLength + valueLength + 8;

    return new Tuple(key, value, offset + recordLength);
  }

  public synchronized void put(K key, V value) {
    File file = new File(dataFolderPath, String.valueOf(activeSegmentId) + initialVersion);
    long fileSizeInKB = file.length() / 1024;
    if (fileSizeInKB < segmentSizeThreshold) {
      writeRecordAndUpdateKeyDir(file, key, value);
    } else {
      File newSegment = new File(dataFolderPath, String.valueOf(++activeSegmentId) + initialVersion);
      writeRecordAndUpdateKeyDir(newSegment, key, value);
    }
  }

  private synchronized void writeRecordAndUpdateKeyDir(File file, K key, V value) {

    // TODO: make a global buffered output stream
    try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      // TODO: apply the right streaming
      byte[] keyBytes = convertToByteArray(key);
      byte[] valueBytes = convertToByteArray(value);

      int offset = (int) file.length();
      writeBytesWithLength(keyBytes, fileOutputStream);
      writeBytesWithLength(valueBytes, fileOutputStream);

      bufferedOutputStream.flush();
      bufferedOutputStream.close();

      ValueLocation valueLocation = ValueLocation.builder().file(file.getName())
          .offset(offset)
          .build();

      keyDir.put(key, valueLocation);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /*
   * 
   * Writes a record to the segment and hint file and updates the keyDir
   */
  private synchronized void writeRecordWithHintFile(File file, K key, V value) throws IOException {
    byte[] keyBytes = convertToByteArray(key);
    byte[] valueBytes = convertToByteArray(value);

    try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      int offset = (int) file.length();

      writeBytesWithLength(keyBytes, fileOutputStream);
      writeBytesWithLength(valueBytes, fileOutputStream);

      bufferedOutputStream.flush();
      bufferedOutputStream.close();

      ValueLocation valueLocation = ValueLocation.builder().file(file.getName())
          .offset(offset)
          .build();
      keyDir.put(key, valueLocation);
    }
    File hintFile = new File(file.getAbsolutePath().concat(".hint"));
    writeHintFile(hintFile, key, null);
  }

  private void writeHintFile(File file, K key, ValueLocation valueLocation) throws IOException {
    byte[] keyBytes = convertToByteArray(key);
    byte[] valueLocationBytes = convertToByteArray(valueLocation);

    try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
      writeBytesWithLength(keyBytes, fileOutputStream);
      writeBytesWithLength(valueLocationBytes, fileOutputStream);
    }
  }

  private void writeBytesWithLength(byte[] objBytes, OutputStream o) throws IOException {
    DataOutputStream dos = new DataOutputStream(o);
    dos.writeInt(objBytes.length);
    o.write(objBytes);
  }

  private byte[] convertToByteArray(Object obj) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(obj);
    oos.flush();
    return bos.toByteArray();
  }

  private Object convertFromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bis);
    return ois.readObject();
  }

  public synchronized void compact() {
    ArrayList<File> compactedSegments = new ArrayList<>();
    // second do the compaction on the new copies
    File file = new File(dataFolderPath);
    File[] segmentsToCompact = file.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // split name by . and get the first part

        long segmentId = Long.valueOf(name.split("\\.")[0]);
        return segmentId < activeSegmentId;
      }
    });

    for (File segment : segmentsToCompact) {
      try {
        compactSegment(segment);
        compactedSegments.add(segment);
        createHintFile(segment);
      } catch (ClassNotFoundException | IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    Timer timer = new Timer();
    timer.schedule(new DeleteOldSegmentsTask(compactedSegments), delayBetweenCompactionAndPurgingMS);
    return;
  }

  private synchronized void purgeOldCopies() {
    File file = new File(dataFolderPath);
    File[] segmentsToPruge = file.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        long segmentId = Long.valueOf(name);
        return segmentId < activeSegmentId;
      }
    });

    for (File segment : segmentsToPruge) {
      segment.delete();
    }
  }

  private synchronized void purgeSegment(File segment) {
    segment.delete();
  }

  private void createHintFile(File segment) throws IOException, ClassNotFoundException {
    File hintFile = new File(segment.getParent(), segment.getName().split("\\.")[0] + ".hint");
    hintFile.delete();
    FileOutputStream fileOutptuStream = new FileOutputStream(hintFile, true);
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutptuStream);
    DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);

    FileInputStream fileInputStream = new FileInputStream(segment);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
    DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);

    int offset = 0;

    while (offset < segment.length()) {
      int keyLength = dataInputStream.readInt();
      byte[] keyBytes = new byte[keyLength];
      dataInputStream.read(keyBytes, offset, keyLength);
      int valueLength = dataInputStream.readInt();

      // keep skipping until valueLength amount is skipped.
      // This is because skip is not guaranteed to skip the exact amount of bytes
      // requested
      long skipped = 0;
      while (skipped < valueLength) {
        skipped += dataInputStream.skip(valueLength - skipped);
      }

      offset = offset + keyLength + valueLength + 8;

      dataOutputStream.writeInt(keyLength);
      dataOutputStream.write(keyBytes);
      dataOutputStream.writeInt(offset);

    }
  }

  private synchronized void compactSegment(File oldSegment)
      throws IOException, ClassNotFoundException {
    // open the segment
    FileInputStream fos = new FileInputStream(oldSegment);
    File newSegment = new File(dataFolderPath, getNewSegmentVersionName(oldSegment.getName()));

    // TODO: we only need the key here
    Tuple<K, V> record;
    int offset = 0;
    do {
      // correct assumption: first record always exists
      record = readRecord(fos, offset);
      K key = record.getKey();
      if (keyDir.containsKey(key)) {
        // write the record to the new segment
        writeRecordAndUpdateKeyDir(newSegment, key, record.getValue());
      } else {
        // skip the record
      }
      // update offset
      offset = record.getEndOffset();
    } while (record.getEndOffset() < oldSegment.length());
    fos.close();

  }

  private String getNewSegmentVersionName(String oldSegmentName) {
    // split the segment with .
    // return the first part + "." + the 2nd field + 1
    String temp[] = oldSegmentName.split("\\.");
    return temp[0] + "." + String.valueOf(Integer.valueOf(temp[1]) + 1);
  }

  public int getSegmentSizeThreshold() {
    return segmentSizeThreshold;
  }

  public String getDataFolderPath() {
    return dataFolderPath;
  }

  public ConcurrentHashMap<K, ValueLocation> getKeyDir() {
    ConcurrentHashMap<K, ValueLocation> keyDir = new ConcurrentHashMap<>();
    keyDir.putAll(this.keyDir);
    return keyDir;
  }

  class DeleteOldSegmentsTask extends TimerTask {
    private ArrayList<File> compactedSegments;

    public DeleteOldSegmentsTask(ArrayList<File> compactedSegments) { // constructor that takes data as a parameter
      this.compactedSegments = compactedSegments;
    }

    public void run() {
      compactedSegments.forEach(segment -> purgeSegment(segment));
    }
  }

  class Tuple<K, V> {
    private K key;
    private V value;
    int endOffset;

    public Tuple(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public Tuple(K key, V value, int endOffset) {
      this.key = key;
      this.value = value;
      this.endOffset = (endOffset);
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public Integer getEndOffset() {
      return endOffset;
    }
  }

}

@Builder(toBuilder = true)
class ValueLocation {

  ValueLocation(String file, int offset) {
    this.file = file;
    this.offset = offset;
  }

  private String file;
  private int offset;

  public String getFile() {
    return file;
  }

  public int getOffset() {
    return offset;
  }
}
