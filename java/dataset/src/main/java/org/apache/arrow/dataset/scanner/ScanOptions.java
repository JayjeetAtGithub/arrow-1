/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.scanner;

import org.apache.arrow.dataset.filter.Filter;

/**
 * Options used during scanning.
 */
public class ScanOptions {
  private final String[] columns;
  private final Filter filter;
  private final int fragmentReadAhead;
  private final long batchSize;

  /**
   * Constructor.
   * @param columns Projected columns. Empty for scanning all columns.
   * @param filter Filter
   * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.VectorSchemaRoot}
   */
  protected ScanOptions(String[] columns, Filter filter, int fragmentReadAhead, long batchSize) {
    this.columns = columns;
    this.filter = filter;
    this.fragmentReadAhead = fragmentReadAhead;
    this.batchSize = batchSize;
  }

  public String[] getColumns() {
    return columns;
  }

  public Filter getFilter() {
    return filter;
  }

  public int getFragmentReadAhead() {
    return fragmentReadAhead;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public static class Builder {
    private String[] columns = new String[0];
    private Filter filter = Filter.EMPTY;
    private int fragmentReadAhead = -1;
    private long batchSize = -1L;

    /**
     * Sets columns to include in returned data. Any columns with names not specified here are not returned.
     * If set to an empty list, we return all columns.
     * @param columns columns to return.
     */
    public Builder setColumns(String[] columns) {
      this.columns = columns;
      return this;
    }

    /**
     * Filter rows of data based on given filter.
     * `Filter.EMPTY` is the empty filter, which skips filtering entirely.
     * @param filter filter to apply on all rows of data.
     */
    public Builder setFilter(Filter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Number of datasource files to read ahead when scanning.
     * Set to a value of `-1` to use the Arrow default readahead.
     * Set to a value of `0` to disable fragment readahead.
     * @param fragmentReadAhead Amount of datasource files to read ahead.
     */
    public Builder setFragmentReadAhead(int fragmentReadAhead) {
      this.fragmentReadAhead = fragmentReadAhead;
      return this;
    }

    /**
     * Maximum size (in bytes) for a single returned batch of data.
     * @param batchSize maximum size (in bytes) of a single returned batch of data.
     */
    public Builder setBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Builds an immutable `ScanOptions` object. Can be called repeatedly without side effects.
     * @return created `ScanOptions` object.
     */
    public ScanOptions build() {
      return new ScanOptions(columns, filter, fragmentReadAhead, batchSize);
    }
  }
}
