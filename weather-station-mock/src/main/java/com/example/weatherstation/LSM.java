package com.example.weatherstation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Optional;

import lombok.Builder;

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
  private String initialVersion = ".1";

  /*
   * Segment size threshold in kilo bytes
   */
  private int segmentSizeThreshold = 10;

  /*
   * Path of the folder that contains the segments
   */
  private String dataFolderPath = System.getProperty("user.dir").concat(File.separator + Constants.LSM_DATA_FOLDER);

  /*
   * Id of the last segment file being written to. (auto incrementing id)
   */
  private long activeSegmentId = 0;

  private int delayBetweenCompactionAndPurgingMS = 3600000;

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
      try {
        writeRecordWithHint(file, key, value);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else {
      File newSegment = new File(dataFolderPath, String.valueOf(++activeSegmentId) + initialVersion);
      try {
        writeRecordWithHint(newSegment, key, value);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private synchronized void writeRecordAndUpdateKeyDir(File file, K key, V value) throws IOException {
    // TODO: apply the right streaming
    byte[] keyBytes = convertToByteArray(key);
    byte[] valueBytes = convertToByteArray(value);

    // TODO: make a global buffered output stream
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
  }

  /*
   * Writes a record to the segment and hint file and updates the keyDir
   */
  private synchronized void writeRecordWithHint(File file, K key, V value) throws IOException {
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

      File hintFile = new File(file.getAbsolutePath().concat(".hint"));
      writeHintFile(hintFile, key, offset);
    }

  }

  /*
   * Writes a record to the segment and hint file and updates the keyDir only if
   * the old keyDir offset is equal to the current offset
   */
  private synchronized void replaceRecordWithHint(File file, K key, V value) throws IOException {
    byte[] keyBytes = convertToByteArray(key);
    byte[] valueBytes = convertToByteArray(value);
    ValueLocation oldValueLocation = keyDir.get(key);
    if (oldValueLocation == null) {
      writeRecordWithHint(file, key, value);
      return;
    }
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
      boolean success = keyDir.replace(key, oldValueLocation, valueLocation);

      if (success) {
        File hintFile = new File(file.getAbsolutePath().concat(".hint"));
        writeHintFile(hintFile, key, offset);
      }
    }
  }

  private void writeHintFile(File file, K key, int offset) throws IOException {
    byte[] keyBytes = convertToByteArray(key);
    FileOutputStream fileOutputStream = new FileOutputStream(file, true);
    writeBytesWithLength(keyBytes, fileOutputStream);
    DataOutputStream dos = new DataOutputStream(fileOutputStream);
    dos.writeInt(offset);
    dos.close();
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

  public synchronized void compact() throws IOException, ClassNotFoundException {
    ArrayList<File> compactedSegments = new ArrayList<>();
    // second do the compaction on the new copies
    File file = new File(dataFolderPath);
    File[] segmentsToCompact = file.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // split name by . and get the first part

        long segmentId = Long.valueOf(name.split("\\.")[0]);
        return segmentId < activeSegmentId && name.split("\\.").length == 2;
      }
    });

    sortFiles(segmentsToCompact);

    File currMergeSegment = null;

    for (File segment : segmentsToCompact) {
      if (currMergeSegment == null) {
        currMergeSegment = new File(dataFolderPath, getNewSegmentVersionName(segment.getName()));
      } else if (currMergeSegment.length() / 1024 > segmentSizeThreshold) {
        currMergeSegment = new File(dataFolderPath, getNextSegment(currMergeSegment.getName()));
      } else {
        currMergeSegment = currMergeSegment;
      }

      compactSegment(currMergeSegment, segment);
      compactedSegments.add(segment);
      // delete old hint file if it exists
      File oldHintFile = new File(segment.getAbsolutePath().concat(".hint"));
      oldHintFile.delete();
    }

    Timer timer = new Timer();
    timer.schedule(new DeleteOldSegmentsTask(compactedSegments), delayBetweenCompactionAndPurgingMS);
    return;
  }

  /*
   * merges(adds) file2 to file1
   */
  // private File mergeFiles(File file1, File file2) throws
  // ClassNotFoundException, IOException {
  // FileInputStream fileInputStream = new FileInputStream(file2);
  // FileOutputStream fileOutputStream = new FileOutputStream(file2, true);

  // Tuple<K, V> record;
  // int offset = 0;
  // while (offset < file2.length()) {
  // record = readRecord(fileInputStream, offset);
  // writeRecordWithHint(file1, record.key, record.value);
  // offset = record.endOffset;
  // }
  // fileOutputStream.close();
  // }

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

  public ConcurrentHashMap<K, ValueLocation> generateKeyDirFromDisk() throws IOException, ClassNotFoundException {
    File file = new File(dataFolderPath);
    ConcurrentHashMap<K, ValueLocation> newKeyDir = new ConcurrentHashMap<K, ValueLocation>();
    // create an arrayList of files of file children
    List<File> allDataFiles = Arrays.asList(file.listFiles());
    sortFiles(allDataFiles);

    List<File> hintFiles = allDataFiles.stream().filter(f -> f.getName().endsWith(".hint")
        && Long.valueOf(f.getName().split("\\.")[0]) < activeSegmentId)
        .collect(Collectors.toList());
    Set<String> hintFilesIdSet = hintFiles.stream().map(f -> f.getName().split("\\.")[0])
        .collect(Collectors.toSet());

    List<File> segmentFiles = allDataFiles.stream().filter(f -> !f.getName().endsWith(".hint"))
        .collect(Collectors.toList());
    Set<String> segmentIdsSet = segmentFiles.stream().map(f -> f.getName().split("\\.")[0])
        .collect(Collectors.toSet());

    /*
     * recovery segments can have older version segments, that's why we
     * getLatestVersionFile
     */
    List<File> recoverySegmentFiles = segmentIdsSet.stream()
        .filter(id -> !hintFilesIdSet.contains(id))
        .map(id -> getLatestVersionFile(id, segmentFiles))
        .collect(Collectors.toList());

    /*
     * 
     * compacted segments don't have older version hint files
     */
    for (File hintFile : hintFiles) {

      FileInputStream fileInputStream = new FileInputStream(hintFile);
      BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
      try (DataInputStream dataInputStream = new DataInputStream(bufferedInputStream)) {
        int offset = 0;

        while (offset < hintFile.length()) {
          int keyLength = dataInputStream.readInt();
          byte[] keyBytes = new byte[keyLength];
          dataInputStream.read(keyBytes);
          int recordOffset = dataInputStream.readInt();

          K key = (K) convertFromByteArray(keyBytes);
          ValueLocation valueLocation = new ValueLocation(
              convertHintFileNameToSegmentName(hintFile.getName()),
              recordOffset);
          newKeyDir.put(key, valueLocation);

          offset += keyLength + 8;
        }
      }

    }

    // sort the segment files by id and version

    for (File segmentFile : recoverySegmentFiles) {

      FileInputStream fileInputStream = new FileInputStream(segmentFile);
      BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
      try (DataInputStream dataInputStream = new DataInputStream(bufferedInputStream)) {
        int offset = 0;

        while (offset < segmentFile.length()) {
          int keyLength = dataInputStream.readInt();
          byte[] keyBytes = new byte[keyLength];
          dataInputStream.read(keyBytes);
          int valueLength = dataInputStream.readInt();
          byte[] valueBytes = new byte[valueLength];
          dataInputStream.read(valueBytes);

          K key = (K) convertFromByteArray(keyBytes);
          // V value = (V) convertFromByteArray(valueBytes);
          ValueLocation valueLocation = new ValueLocation(segmentFile.getName(),
              offset);
          newKeyDir.put(key, valueLocation);

          offset += keyLength + valueLength + 8;
        }
      } catch (FileNotFoundException e) {
        throw e;
      } catch (ClassNotFoundException | IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }

    keyDir = newKeyDir;
    ConcurrentHashMap<K, ValueLocation> returnKeyDir = new ConcurrentHashMap<>();
    returnKeyDir.putAll(newKeyDir);
    return returnKeyDir;
  }

  private String convertHintFileNameToSegmentName(String hintFileName) {
    String[] split = hintFileName.split("\\.");
    return split[0] + "." + split[1];
  }

  private void sortFiles(List<File> files) {
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        String[] o1Split = o1.getName().split("\\.");
        String[] o2Split = o2.getName().split("\\.");
        int o1Id = Integer.valueOf(o1Split[0]);
        int o2Id = Integer.valueOf(o2Split[0]);
        int o1Version = Integer.valueOf(o1Split[1]);
        int o2Version = Integer.valueOf(o2Split[1]);
        if (o1Id == o2Id) {
          return o1Version - o2Version;
        } else {
          return o1Id - o2Id;
        }
      }
    });
  }

  private void sortFiles(File[] files) {
    Arrays.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        String[] o1Split = o1.getName().split("\\.");
        String[] o2Split = o2.getName().split("\\.");
        int o1Id = Integer.valueOf(o1Split[0]);
        int o2Id = Integer.valueOf(o2Split[0]);
        int o1Version = Integer.valueOf(o1Split[1]);
        int o2Version = Integer.valueOf(o2Split[1]);
        if (o1Id == o2Id) {
          return o1Version - o2Version;
        } else {
          return o1Id - o2Id;
        }
      }
    });
  }

  private File getLatestVersionFile(String segmentId, List<File> segmentFiles) {
    List<File> segmentFilesWithId = segmentFiles.stream().filter(f -> f.getName().startsWith(segmentId + "."))
        .collect(Collectors.toList());
    return segmentFilesWithId.stream().max((f1, f2) -> f1.getName().compareTo(f2.getName())).get();
  }

  private void createHintFile(File segment) throws IOException, ClassNotFoundException {
    File hintFile = new File(segment.getParent(), segment.getName().split("\\.")[0] + ".hint");
    hintFile.delete();
    FileOutputStream fileOutptuStream = new FileOutputStream(hintFile, true);
    DataOutputStream dataOutputStream = new DataOutputStream(fileOutptuStream);

    FileInputStream fileInputStream = new FileInputStream(segment);
    DataInputStream dataInputStream = new DataInputStream(fileInputStream);

    int offset = 0;

    while (offset < segment.length()) {
      int keyLength = dataInputStream.readInt();
      byte[] keyBytes = new byte[keyLength];
      fileInputStream.read(keyBytes);
      int valueLength = dataInputStream.readInt();
      byte[] valueBytes = new byte[valueLength];
      fileInputStream.read(valueBytes);

      offset = offset + keyLength + valueLength + 8;

      dataOutputStream.writeInt(keyLength);
      dataOutputStream.write(keyBytes);
      dataOutputStream.writeInt(offset);

    }
  }

  private synchronized void compactSegment(File segmentToMergeTo, File oldSegment)
      throws IOException, ClassNotFoundException {
    // open the segment
    FileInputStream fos = new FileInputStream(oldSegment);

    // TODO: we only need the key here
    Tuple<K, V> record;
    int offset = 0;
    while (offset < oldSegment.length()) {
      record = readRecord(fos, offset);
      K key = record.getKey();
      if (keyDir.containsKey(key)) {
        // write the record to the new segment
        // TODO: use replace here
        replaceRecordWithHint(segmentToMergeTo, key, record.getValue());
      } else {
        // skip the record
      }
      // update offset
      offset = record.getEndOffset();
    }
    fos.close();

  }

  private String getNewSegmentVersionName(String oldSegmentName) {
    // split the segment with .
    // return the first part + "." + the 2nd field + 1
    String temp[] = oldSegmentName.split("\\.");
    return temp[0] + "." + String.valueOf(Integer.valueOf(temp[1]) + 1);
  }

  private String getNextSegment(String oldSegmentName) {
    String temp[] = oldSegmentName.split("\\."); // 0 -> segment id , 1 -> segment id version
    return String.valueOf(Integer.valueOf(temp[0]) + 1) + "." + temp[1];
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

  public static class Builder<K, V> {
    private String initialVersion;
    private int segmentSizeThreshold;
    private String dataFolderPath;
    private long activeSegmentId;
    private int delayBetweenCompactionAndPurgingMS;
    private ConcurrentHashMap<K, ValueLocation> keyDir;

    public Builder() {
      // Set default values if needed
      this.initialVersion = ".1";
      this.segmentSizeThreshold = 10;
      this.dataFolderPath = System.getProperty("user.dir").concat(File.separator + Constants.LSM_DATA_FOLDER);
      this.activeSegmentId = 0;
      this.delayBetweenCompactionAndPurgingMS = 3600000;
      this.keyDir = new ConcurrentHashMap<>();
    }

    public Builder<K, V> initialVersion(String initialVersion) {
      this.initialVersion = initialVersion;
      return this;
    }

    public Builder<K, V> segmentSizeThreshold(int segmentSizeThreshold) {
      this.segmentSizeThreshold = segmentSizeThreshold;
      return this;
    }

    public Builder<K, V> dataFolderPath(String dataFolderPath) {
      this.dataFolderPath = dataFolderPath;
      return this;
    }

    public Builder<K, V> activeSegmentId(long activeSegmentId) {
      this.activeSegmentId = activeSegmentId;
      return this;
    }

    public Builder<K, V> delayBetweenCompactionAndPurgingMS(int delayBetweenCompactionAndPurgingMS) {
      this.delayBetweenCompactionAndPurgingMS = delayBetweenCompactionAndPurgingMS;
      return this;
    }

    public Builder<K, V> keyDir(ConcurrentHashMap<K, ValueLocation> keyDir) {
      this.keyDir = keyDir;
      return this;
    }

    public LSM<K, V> build() throws ClassNotFoundException, IOException {
      LSM<K, V> myClass = new LSM<>();
      myClass.initialVersion = this.initialVersion;
      myClass.segmentSizeThreshold = this.segmentSizeThreshold;
      myClass.dataFolderPath = this.dataFolderPath;
      myClass.delayBetweenCompactionAndPurgingMS = this.delayBetweenCompactionAndPurgingMS;
      myClass.keyDir = this.keyDir;
      myClass.activeSegmentId = getLatestSegmentId(dataFolderPath);
      myClass.generateKeyDirFromDisk();
      return myClass;
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
