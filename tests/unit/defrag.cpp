#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE DEFRAG library test

#include <boost/test/included/unit_test.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <functional>
#include <random>
#include <utility>
#include <vector>

#include <err.h>
#include <sysexits.h>

#include "eblob/eblob.hpp"
#include "eblob_wrapper.h"
#include "library/blob.h"
#include "library/datasort.h"
#include "library/list.h"


eblob_config_test_wrapper initialize_eblob_config_for_defrag() {
	eblob_config_test_wrapper wrapper;
	auto &config = wrapper.config;
	constexpr size_t RECORDS_IN_BLOB = 10;
	config.records_in_blob = RECORDS_IN_BLOB;
	config.blob_size = 10 * (1ULL << 30); // 10Gib
	config.defrag_timeout = 0; // we don't want to autodefrag
	config.defrag_time = 0;
	config.defrag_splay = 0;
	config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS | EBLOB_USE_VIEWS;
	return wrapper;
}


class iterator_private {
public:
	explicit iterator_private(std::vector<item_t> &items_)
	: items(items_)
	, expect_blob_sorted(true) {
	}

	explicit iterator_private(std::vector<item_t> &items_, bool expect_blob_sorted_)
	: items(items_)
	, expect_blob_sorted(expect_blob_sorted_) {
	}

	std::vector<item_t> &items;
	bool expect_blob_sorted;
	size_t number_checked = 0;
	size_t prev_offset = 0;

	bool check_dc_positions = false;
	int last_dc_position = 0;
};


int iterate_callback(struct eblob_disk_control *dc,
                     struct eblob_ram_control *rctl __attribute_unused__,
                     int fd,
                     uint64_t data_offset,
                     void *priv,
                     void *thread_priv __attribute_unused__) {
	// TODO: BOOST_REQUIRE_EQUAL used because of 54-th version of boost
	// Maybe it will be more comfortable with BOOST_TEST and error messages in more modern boost version.
	BOOST_REQUIRE(!(dc->flags & BLOB_DISK_CTL_REMOVE));  // removed dc occured

	iterator_private &ipriv = *static_cast<iterator_private *>(priv);
	if (ipriv.expect_blob_sorted) {
		BOOST_REQUIRE_GT(data_offset, ipriv.prev_offset);
		ipriv.prev_offset = data_offset;
	}

	auto &items = ipriv.items;
	BOOST_REQUIRE_LT(ipriv.number_checked, items.size()); // index out of range

	auto &item = items[ipriv.number_checked];
	BOOST_REQUIRE(!item.removed);  // item removed
	BOOST_REQUIRE(!item.checked); //  item already checked
	BOOST_REQUIRE_EQUAL(dc->data_size, item.value.size());  // sizes mismatch

	if (ipriv.check_dc_positions) {
		BOOST_REQUIRE_LE(ipriv.last_dc_position, dc->position);
		ipriv.last_dc_position = dc->position;
	}

	std::vector<char> data(dc->data_size);
	int ret = __eblob_read_ll(fd, data.data(), dc->data_size, data_offset);
	BOOST_REQUIRE_EQUAL(ret, 0);  // can't read data
	BOOST_REQUIRE(data == item.value);  // content of the value differs

	item.checked = true;
	++ipriv.number_checked;
	return 0;
}


struct bases_stats {
	size_t number_blobs = 0;
	size_t size_alive_data = 0;
	size_t size_removed_data = 0;
	size_t number_alive_records = 0;
	size_t number_removed_records = 0;
};


bases_stats calculate_bases_stats_by_bases(eblob_base_ctl **bctls, size_t bctl_cnt) {
	bases_stats stats;
	stats.number_blobs = bctl_cnt;
	for (size_t index = 0; index != bctl_cnt; ++index) {
		eblob_base_ctl *bctl = bctls[index];
		size_t number_records = eblob_stat_get(bctl->stat, EBLOB_LST_RECORDS_TOTAL);
		size_t removed_records = eblob_stat_get(bctl->stat, EBLOB_LST_RECORDS_REMOVED);
		stats.number_alive_records += number_records - removed_records;
		stats.number_removed_records += removed_records;
		size_t datasize = eblob_stat_get(bctl->stat, EBLOB_LST_BASE_SIZE);
		size_t removed_datasize = eblob_stat_get(bctl->stat, EBLOB_LST_REMOVED_SIZE);
		stats.size_alive_data += datasize - removed_datasize;
		stats.size_removed_data += removed_datasize;
	}

	return stats;
}

void check_defrag_stats(eblob_wrapper &wrapper, size_t sorted_blob_number,
                        size_t unsorted_blob_number, bases_stats &stats) {
	auto total_blob_number = sorted_blob_number + unsorted_blob_number;
	auto &cfg = wrapper.get()->cfg;
	eblob_stat *stat = wrapper.get()->stat;
	int view_used = eblob_stat_get(stat, EBLOB_GST_DATASORT_VIEW_USED_NUMBER);
	int sorted_view_used = eblob_stat_get(stat, EBLOB_GST_DATASORT_SORTED_VIEW_USED_NUMBER);
	int sp_view_used = eblob_stat_get(stat, EBLOB_GST_DATASORT_SINGLE_PASS_VIEW_USED_NUMBER);
	size_t number_touched_blobs = eblob_stat_get(stat, EBLOB_GST_DATASORT_BLOBS_NUMBER);
	size_t size_alive_data = eblob_stat_get(stat, EBLOB_GST_DATASORT_ALIVE_DATA_SIZE);
	size_t size_removed_data = eblob_stat_get(stat, EBLOB_GST_DATASORT_REMOVED_DATA_SIZE);
	size_t number_alive_records = eblob_stat_get(stat, EBLOB_GST_DATASORT_ALIVE_RECORDS_NUMBER);
	size_t number_removed_records = eblob_stat_get(stat, EBLOB_GST_DATASORT_REMOVED_RECORDS_NUMBER);

	if (!(cfg.blob_flags & EBLOB_USE_VIEWS)) {
		BOOST_REQUIRE_EQUAL(view_used, 0);
		BOOST_REQUIRE_EQUAL(sorted_view_used, 0);
		BOOST_REQUIRE_EQUAL(sp_view_used, 0);
	} else if (cfg.single_pass_file_size_threshold == 1) {
		BOOST_REQUIRE_EQUAL(view_used, total_blob_number);
		BOOST_REQUIRE_EQUAL(sorted_view_used, sorted_blob_number);
		BOOST_REQUIRE_EQUAL(sp_view_used, unsorted_blob_number);
	} else if (cfg.single_pass_file_size_threshold == 0) {
		BOOST_REQUIRE_EQUAL(view_used, sorted_blob_number);
		BOOST_REQUIRE_EQUAL(sorted_view_used, sorted_blob_number);
		BOOST_REQUIRE_EQUAL(sp_view_used, 0);
	} else {
		BOOST_FAIL("unsupported single_pass_file_size_threshold param in test");
	}

	BOOST_REQUIRE_EQUAL(number_touched_blobs, stats.number_blobs);
	BOOST_REQUIRE_EQUAL(size_alive_data, stats.size_alive_data);
	BOOST_REQUIRE_EQUAL(size_removed_data, stats.size_removed_data);
	BOOST_REQUIRE_EQUAL(number_alive_records, stats.number_alive_records);
	BOOST_REQUIRE_EQUAL(number_removed_records, stats.number_removed_records);
}

int datasort(eblob_wrapper &wrapper, const std::set<size_t> &indexes) {
	size_t number_bases = indexes.size();
	BOOST_REQUIRE(!indexes.empty());

	// collect required bctls according to passed indexes
	std::vector<eblob_base_ctl *> bctls;
	bctls.reserve(number_bases);
	eblob_base_ctl *bctl;
	size_t loop_index = 0;
	size_t sorted_count = 0;
	size_t unsorted_count = 0;
	list_for_each_entry(bctl, &wrapper.get()->bases, base_entry) {
		if (indexes.count(loop_index)) {
			// count number of sorted and unsorted chunks in this datasort to check stat later
			if (datasort_base_is_sorted(bctl)) {
				sorted_count++;
			} else {
				unsorted_count++;
			}
			bctls.emplace_back(bctl);
		}

		++loop_index;
		if (bctls.size() == number_bases)
			break;
	}

	BOOST_REQUIRE(bctls.size() == number_bases);

	// prepare config for datasort
	datasort_cfg dcfg;
	memset(&dcfg, 0, sizeof(dcfg));
	dcfg.b = wrapper.get();
	dcfg.log = dcfg.b->cfg.log;
	dcfg.bctl = bctls.data();
	dcfg.bctl_cnt = bctls.size();

	bases_stats stats = calculate_bases_stats_by_bases(dcfg.bctl, dcfg.bctl_cnt);
	// run defrag on selected blobs
	eblob_defrag_reset_stats(dcfg.b);
	auto result = eblob_generate_sorted_data(&dcfg);

	// check from stats that views were used appropriate number of times
	check_defrag_stats(wrapper, sorted_count, unsorted_count, stats);
	return result;
}


int iterate(eblob_wrapper &wrapper,
            iterator_private &priv,
            int iterate_flags = EBLOB_ITERATE_FLAGS_ALL | EBLOB_ITERATE_FLAGS_READONLY) {
	eblob_iterate_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.iterator = iterate_callback;

	eblob_iterate_control ictl;
	memset(&ictl, 0, sizeof(struct eblob_iterate_control));
	ictl.b = wrapper.get();
	ictl.log = ictl.b->cfg.log;
	ictl.flags = iterate_flags;
	ictl.iterator_cb = callbacks;
	ictl.priv = &priv;
	return eblob_iterate(wrapper.get(), &ictl);
}

void filter_items(std::vector<item_t> &items) {
	auto it = std::remove_if(items.begin(), items.end(), [](const item_t &item) -> bool {
		return item.removed;
	});
	items.erase(it, items.end());
}

void filter_and_sort_items(std::vector<item_t> &items) {
	filter_items(items);
	std::sort(items.begin(), items.end());
}

void run_with_different_modes(std::function<void(const eblob_config &)> runnable) {
	auto cw = initialize_eblob_config_for_defrag();

	// Enable views
	BOOST_TEST_CHECKPOINT("running with enabled views");

	// Enable views, but not single pass
	BOOST_TEST_CHECKPOINT("running with zero single_pass_file_size_threshold");
	cw.config.single_pass_file_size_threshold = 0;
	runnable(cw.config);

	// min non-zero value: single-pass is always on
	BOOST_TEST_CHECKPOINT("running with 1 single_pass_file_size_threshold");
	cw.reset_dirs();
	cw.config.single_pass_file_size_threshold = 1;
	runnable(cw.config);

	// disable views
	BOOST_TEST_CHECKPOINT("running with views disabled");
	cw.reset_dirs();
	cw.config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS;
	runnable(cw.config);

	BOOST_TEST_CHECKPOINT("running with chunk splitting by position");
	cw.reset_dirs();
	cw.config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS | EBLOB_SORT_BY_POS;
	runnable(cw.config);
}

/**
 * 1) Make two bases with 10 records each.
 *    State: data.0(unsorted, 10 records), data.1(unsorted, 10 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 10 records), data.1(unsorted, 10 records)
 * 3) Remove half of first base.
 *    State: data.0(sorted, 5 records), data.1(unsorted, 10 records)
 * 4) Defrag first base.
 *    State: data.0(sorted with view, 5 records), data.1(unsorted, 10 records)
 * 5) Check that bases contains all 15 records
 */
void run_first_base_sorted_second_base_unsorted(const eblob_config &eblob_config);
BOOST_AUTO_TEST_CASE(first_base_sorted_second_base_unsorted) {
	run_with_different_modes(run_first_base_sorted_second_base_unsorted);
}

void run_first_base_sorted_second_base_unsorted(const eblob_config &eblob_config) {
	const size_t RECORDS_IN_BLOB = eblob_config.records_in_blob;
	const size_t TOTAL_RECORDS = 2 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE = RECORDS_IN_BLOB / 2;

	eblob_wrapper wrapper(eblob_config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	auto generator = make_default_item_generator(wrapper);
	std::vector<item_t> shadow_elems;

	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);

	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	// Remove a half items from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	filter_items(shadow_elems);
	// partial sort
	std::sort(shadow_elems.begin(), shadow_elems.begin() + (RECORDS_IN_BLOB - RECORDS_TO_REMOVE));

	// TODO: need to check that we use view over base
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	iterator_private priv(shadow_elems, false);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - RECORDS_TO_REMOVE);
}


/**
 * 1) Make two bases with 10 records each.
 *    State: data.0(unsorted, 10 records), data.1(unsorted, 10 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 10 records), data.1(unsorted, 10 records)
 * 3) Remove a half from each base
 *    State: data.0(sorted, 5 records), data.1(unsorted, 5 records)
 * 4) Defrag two bases with view on first base
 *    State: data.0(sorted, 10 records)
 * 5) Check that result base contains 10 records
 */
void run_merge_sorted_and_unsorted_bases(const eblob_config &config);
BOOST_AUTO_TEST_CASE(merge_sorted_and_unsorted_bases) {
	run_with_different_modes(run_merge_sorted_and_unsorted_bases);
}

void run_merge_sorted_and_unsorted_bases(const eblob_config &config) {
	const size_t TOTAL_RECORDS = 2 * config.records_in_blob;
	const size_t RECORDS_TO_REMOVE_IN_BASE = config.records_in_blob / 2;

	eblob_wrapper wrapper(config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	auto generator = make_default_item_generator(wrapper);
	std::vector<item_t> shadow_elems;

	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);

	// Sort first base
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[config.records_in_blob + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);
	filter_and_sort_items(shadow_elems);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make two bases with 10 records each.
 *     State: data.0(unsorted, 10 record), data.1(unsorted, 10 records)
 *  2) Defrag two bases separately
 *     State: data.0(sorted, 10 records), data.1(sorted, 10 records)
 *  3) Remove a half from each base
 *     State: data.0(sorted, 5 records), data.1(sorted, 5 records)
 *  4) Defrag two bases with view on first base
 *     State: data.0(sorted, 10 records)
 *  5) Check that result base contains 10 records
 */
void run_merge_sorted_and_sorted_bases(const eblob_config &);
BOOST_AUTO_TEST_CASE(merge_sorted_and_sorted_bases) {
	run_with_different_modes(run_merge_sorted_and_sorted_bases);
}

void run_merge_sorted_and_sorted_bases(const eblob_config &config) {
	const size_t TOTAL_RECORDS = 2 * config.records_in_blob;
	const size_t RECORDS_TO_REMOVE_IN_BASE = config.records_in_blob / 2;

	eblob_wrapper wrapper(config);

	BOOST_REQUIRE(wrapper.get() != nullptr);

	auto generator = make_default_item_generator(wrapper);
	std::vector<item_t> shadow_elems;

	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);

	// Sort bases separately
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {1}), 0);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[config.records_in_blob + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);

	filter_and_sort_items(shadow_elems);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make two bases with 10 records each.
 *     State: data.0(unsorted, 10 record), data.1(unsorted, 10 records)
 *  3) Remove a half from each base
 *     State: data.0(unsorted, 5 records), data.1(unsorted, 5 records)
 *  4) Defrag two bases without view on bases
 *     State: data.0(sorted, 10 records)
 *  5) Check that result base contains 10 records
 */
void run_merge_unsorted_and_unsorted_bases(const eblob_config &config);
BOOST_AUTO_TEST_CASE(merge_unsorted_and_unsorted_bases) {
	run_with_different_modes(run_merge_unsorted_and_unsorted_bases);
}

void run_merge_unsorted_and_unsorted_bases(const eblob_config &config) {
	const size_t TOTAL_RECORDS = 2 * config.records_in_blob;
	const size_t RECORDS_TO_REMOVE_IN_BASE = config.records_in_blob / 2;

	eblob_wrapper wrapper(config);

	BOOST_REQUIRE(wrapper.get() != nullptr);

	auto generator = make_default_item_generator(wrapper);
	std::vector<item_t> shadow_elems;

	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[config.records_in_blob + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);

	filter_and_sort_items(shadow_elems);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make three bases with 10 records each.
 *     State: data.0(unsorted, 10 record), data.1(unsorted, 10 records), data.2(unsorted, 10 records)
 *  3) Remove elements from two bases
 *     State: data.0(unsorted, 0 records), data.1(unsorted, 0 records), data.2(unsorted, 10 records)
 *  4) Defrag all bases
 *     State: data.2(unsorted, 10 records)
 *  5) Check that result base contains 10 records
 */
void run_remove_bases(const eblob_config &config);
BOOST_AUTO_TEST_CASE(remove_bases) {
	run_with_different_modes(run_remove_bases);
}

void run_remove_bases(const eblob_config &config) {
	const size_t TOTAL_RECORDS = 3 * config.records_in_blob;
	const size_t RECORDS_TO_REMOVE = 2 * config.records_in_blob;

	eblob_wrapper wrapper(config);

	BOOST_REQUIRE(wrapper.get() != nullptr);

	auto generator = make_default_item_generator(wrapper);
	std::vector<item_t> shadow_elems;

	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);

	// Sort bases separately
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {1}), 0);

	// Remove all elements
	for (size_t index = 0; index != RECORDS_TO_REMOVE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Defrag eblob (last base should not be touched)
	BOOST_REQUIRE_EQUAL(eblob_defrag(wrapper.get()), 0);
	filter_items(shadow_elems);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - RECORDS_TO_REMOVE);
}

/*
 * Trigger defrag & compact with and without chunks_dir and check that it changes internal
 * state for next defrag. This test doesn't execute defrag.
 */
BOOST_AUTO_TEST_CASE(test_defrag_trigger) {
	eblob_config_test_wrapper config_wrapper;
	config_wrapper.config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS;
	config_wrapper.config.chunks_dir = nullptr;

	eblob_wrapper wrapper(config_wrapper.config);

	auto backend = wrapper.get();

	BOOST_REQUIRE(backend->defrag_chunks_dir == nullptr);
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_NOT_STARTED);

	const std::string defrag_dir = config_wrapper.base_dir() + "/defrag_dir";

	// remove this flag to allow start_defrag to trigger background defrag
	backend->cfg.blob_flags &= ~EBLOB_DISABLE_THREADS;

	// check that triggered data sort sets defrag_dir as chunks_dir for next defrag
	BOOST_REQUIRE_EQUAL(eblob_start_defrag_in_dir(backend, EBLOB_DEFRAG_STATE_DATA_SORT, defrag_dir.c_str()), 0);
	// check that defrag_dir was set into internal state
	BOOST_REQUIRE_EQUAL(std::string(backend->defrag_chunks_dir), defrag_dir);
	// check that data_sort was triggered
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_DATA_SORT);

	// stop defrag
	BOOST_REQUIRE_EQUAL(eblob_stop_defrag(backend), 0);
	// check that chunks_dir was reset
	BOOST_REQUIRE(backend->defrag_chunks_dir == nullptr);
	// check that data_sort was stopped
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_NOT_STARTED);


	// check that triggered compact sets defrag_dir as chunks_dir for next defrag
	BOOST_REQUIRE_EQUAL(eblob_start_defrag_in_dir(backend, EBLOB_DEFRAG_STATE_DATA_COMPACT, defrag_dir.c_str()), 0);
	// check that defrag_dir was set into internal state
	BOOST_REQUIRE_EQUAL(std::string(backend->defrag_chunks_dir), defrag_dir);
	// check that data_compact was triggered
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_DATA_COMPACT);

	// stop defrag
	BOOST_REQUIRE_EQUAL(eblob_stop_defrag(backend), 0);
	// check that chunks_dir was reset
	BOOST_REQUIRE(backend->defrag_chunks_dir == nullptr);
	// check that data_sort was stopped
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_NOT_STARTED);


	// check that triggered data sort sets nullptr as chunks_dir for next defrag
	BOOST_REQUIRE_EQUAL(eblob_start_defrag_in_dir(backend, EBLOB_DEFRAG_STATE_DATA_SORT, nullptr), 0);
	// check that chunks_dir remain unset
	BOOST_REQUIRE(backend->defrag_chunks_dir == nullptr);
	// check that data_sort was triggered
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_DATA_SORT);

	// stop defrag
	BOOST_REQUIRE_EQUAL(eblob_stop_defrag(backend), 0);
	// check that chunks_dir was reset
	BOOST_REQUIRE(backend->defrag_chunks_dir == nullptr);
	// check that data_sort was stopped
	BOOST_REQUIRE_EQUAL(backend->want_defrag, EBLOB_DEFRAG_STATE_NOT_STARTED);
}

/*
 * Run defrag in specified chunks dir, wait its completion and check status
 */
BOOST_AUTO_TEST_CASE(test_defrag_in_dir) {
	eblob_config_test_wrapper config_wrapper;
	config_wrapper.config.blob_flags = EBLOB_L2HASH;
	config_wrapper.config.chunks_dir = nullptr;
	config_wrapper.config.records_in_blob = 10;
	config_wrapper.config.blob_size = 1ULL << 30;

	std::string config_chunks_dir = config_wrapper.base_dir() + "/config_chunks_dir";
	BOOST_REQUIRE_EQUAL(mkdir(config_chunks_dir.c_str(), S_IREAD), 0);
	config_wrapper.config.chunks_dir = &config_chunks_dir.front();

	eblob_wrapper wrapper(config_wrapper.config);

	auto backend = wrapper.get();

	// fill up 2 blobs
	for (size_t i = 0; i < config_wrapper.config.records_in_blob * 2; ++i) {
		BOOST_REQUIRE_EQUAL(eblob_write_hashed(backend, &i, sizeof(i), &i, 0, sizeof(i), 0), 0);
	}

	// create defrag_dir directory
	const std::string defrag_dir = config_wrapper.base_dir() + "/defrag_dir";
	BOOST_REQUIRE_EQUAL(mkdir(defrag_dir.c_str(), S_IRWXU), 0);

	// trigger data_sort in defrag_dir
	BOOST_REQUIRE_EQUAL(eblob_start_defrag_in_dir(backend, EBLOB_DEFRAG_STATE_DATA_SORT, defrag_dir.c_str()), 0);

	// wait defrag completion
	while (eblob_defrag_status(backend) == EBLOB_DEFRAG_STATE_DATA_SORT) {
		usleep(1);
	}

	// check that defrag succeeded
	BOOST_REQUIRE_EQUAL(eblob_stat_get(backend->stat, EBLOB_GST_DATASORT_COMPLETION_STATUS), 0);
}


BOOST_AUTO_TEST_CASE(test_defrag_by_position) {
	const size_t TOTAL_RECORDS = 10;
	eblob_config_test_wrapper config_wrapper(true, EBLOB_LOG_INFO);
	config_wrapper.config.records_in_blob = TOTAL_RECORDS;
	config_wrapper.config.blob_flags |= EBLOB_ITERATE_FLAGS_BY_POSITION;

	eblob_wrapper wrapper(config_wrapper.config);
	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 10, 10);
	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	eblob_base_ctl *first_bctl = container_of(wrapper.get()->bases.next, eblob_base_ctl, base_entry);
	BOOST_REQUIRE_EQUAL(eblob_generate_sorted_index(wrapper.get(), first_bctl), 0);
	iterator_private priv(shadow_elems, false);
	priv.check_dc_positions = true;
	int flags = EBLOB_ITERATE_FLAGS_ALL | EBLOB_ITERATE_FLAGS_READONLY | EBLOB_ITERATE_FLAGS_BY_POSITION;
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv, flags), 0);
}
