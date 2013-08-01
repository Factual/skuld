package com.aphyr.skuld;

import java.util.Arrays;

// Immutable (unenforced) byte arrays with sane value semantics.
public class Bytes {
  public final byte[] bytes;
  public final int hashCode;

  public Bytes(final byte[] bytes) {
    this.bytes = bytes;
    this.hashCode = Arrays.hashCode(bytes);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (! (o instanceof Bytes)) {
      return false;
    }

    final Bytes b = (Bytes) o;
    return Arrays.equals(bytes, b.bytes);
  }
}
