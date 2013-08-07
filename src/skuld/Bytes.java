package com.aphyr.skuld;

import java.util.Arrays;
import com.google.common.primitives.UnsignedBytes;

// Immutable (unenforced) byte arrays with sane value semantics.
public class Bytes implements Comparable {
  public final byte[] bytes;
  public final int hashCode;

  public Bytes(final byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException(
          "bytes should have been a byte[], was null");
    }

    this.bytes = bytes;
    this.hashCode = Arrays.hashCode(bytes);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(final Object o) {
    if (! (o instanceof Bytes)) {
      return false;
    }

    final Bytes b = (Bytes) o;
    return Arrays.equals(bytes, b.bytes);
  }

  @Override
  public int compareTo(final Object o) {
    if (! (o instanceof Bytes)) {
      throw new IllegalArgumentException("Can't compare Bytes to" +
          o.toString());
    }
    
    return UnsignedBytes.lexicographicalComparator().
      compare(bytes, ((Bytes) o).bytes);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for(byte b: bytes)
      sb.append(String.format("%02x", b&0xff));
    return sb.toString();
  }
}
