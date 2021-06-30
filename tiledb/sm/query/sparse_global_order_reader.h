/**
 * @file   sparse_global_order_reader.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines class SparseGlobalOrderReader.
 */

#ifndef TILEDB_SPARSE_GLOBAL_ORDER_H
#define TILEDB_SPARSE_GLOBAL_ORDER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/reader_base.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes sparse global order read queries. */
class SparseGlobalOrderReader : public ReaderBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseGlobalOrderReader(
      stats::Stats* stats,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition);

  /** Destructor. */
  ~SparseGlobalOrderReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(SparseGlobalOrderReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SparseGlobalOrderReader);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Finalizes the reader. */
  Status finalize() {
    return Status::Ok();
  }

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /** Initializes the reader. */
  Status init();

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Is the reader done with the query. */
  bool done_loading_coord_tiles_;

  /** The tile index inside of each fragments. */
  std::vector<std::pair<uint64_t, uint64_t>> frag_tile_idx_;

  /** Have we loaded all thiles for this fragment. */
  std::vector<bool> all_tiles_loaded_;

  /** The result cell slabs currently in process. */
  std::vector<ResultCellSlab> result_cell_slabs_;

  /** The result tiles currently loaded. */
  std::vector<std::list<ResultTile>> result_tiles_;

  /**
   * Maintain a temporary vector with pointers to result tiles, so that
   * `read_tiles`, `unfilter_tiles` can work without changes.
   */
  std::vector<ResultTile*> tmp_result_tiles_;

  /** Have ve loaded the initial data. */
  bool initial_data_loaded_;

  /** Bit vector of the tiles in the subarray, if set. */
  std::vector<std::vector<char>> result_tile_set_;

  /** Dimension names. */
  std::vector<std::string> dim_names_;

  /** Total memory budget. */
  uint64_t memory_budget_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Load tile offsets and result tile set. */
  Status load_initial_data();

  /** Populate a result cell slab to process. */
  Status populate_result_cell_slab();

  /** Computes info about the which tiles are in the subarray. */
  Status compute_result_tiles_set(
      Subarray& subarray, std::vector<std::vector<char>>& result_tile_set);

  /**
   * Add a new tile to the queue of tiles currently being processed
   *  for a specific fragment.
   */
  template <class T>
  Status add_next_tile_to_queue(
      bool subarray_set,
      unsigned int frag_idx,
      std::vector<std::list<ResultTile>::iterator>& result_tiles_it,
      std::vector<std::vector<uint8_t>>& tiles_result_bitmap,
      std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>&
          tile_queue,
      T& cmp,
      bool* need_more_tiles);

  /** Computes a tile's Hilbert values, stores them in the comprator. */
  template <class T>
  Status calculate_hilbert_values(
      bool subarray_set,
      ResultTile* tile,
      std::vector<std::vector<uint8_t>>& tiles_result_bitmap,
      T& cmp);

  /** Compute the result bitmap for a tile. */
  Status compute_tile_result_bitmap(
      bool subarray_set,
      ResultTile* tile,
      std::vector<std::vector<uint8_t>>& tiles_result_bitmap);

  /** Compute the result cell slabs once tiles are loaded. */
  template <class T>
  Status compute_result_cell_slabs(T cmp);

  /** Resize the output buffers to the correct size after copying. */
  Status resize_output_buffers(std::vector<ResultCellSlab>& copied);

  /** Clean up processed data after copying. */
  Status clean_up_processed_data(std::vector<ResultCellSlab>& copied);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_GLOBAL_ORDER_H
