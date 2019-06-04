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

template<class D>
void fill_eblob(eblob_wrapper &wrapper,
                std::vector<item_t> &shadow_elems,
                item_generator<D> &generator,
                size_t total_records) {
	for (size_t index = 0; index != total_records; ++index) {
		shadow_elems.push_back(generator.generate_item(index));
		BOOST_REQUIRE_EQUAL(wrapper.insert_item(shadow_elems.back()), 0);
	}
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

	std::vector<char> data(dc->data_size);
	int ret = __eblob_read_ll(fd, data.data(), dc->data_size, data_offset);
	BOOST_REQUIRE_EQUAL(ret, 0);  // can't read data
	BOOST_REQUIRE(data == item.value);  // content of the value differs

	item.checked = true;
	++ipriv.number_checked;
	return 0;
}


int datasort(eblob_wrapper &wrapper, const std::set<size_t> &indexes) {
	size_t number_bases = indexes.size();
	BOOST_REQUIRE(!indexes.empty());

	std::vector<eblob_base_ctl *> bctls;
	bctls.reserve(number_bases);
	eblob_base_ctl *bctl;
	size_t loop_index = 0;
	list_for_each_entry(bctl, &wrapper.get()->bases, base_entry) {
		if (indexes.count(loop_index)) {
			bctls.emplace_back(bctl);
		}

		++loop_index;
		if (bctls.size() == number_bases)
			break;
	}

	BOOST_REQUIRE(bctls.size() == number_bases);
	datasort_cfg dcfg;
	memset(&dcfg, 0, sizeof(dcfg));
	dcfg.b = wrapper.get();
	dcfg.log = dcfg.b->cfg.log;
	dcfg.bctl = bctls.data();
	dcfg.bctl_cnt = bctls.size();
	return eblob_generate_sorted_data(&dcfg);
}


int iterate(eblob_wrapper &wrapper, iterator_private &priv) {
	eblob_iterate_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.iterator = iterate_callback;

	eblob_iterate_control ictl;
	memset(&ictl, 0, sizeof(struct eblob_iterate_control));
	ictl.b = wrapper.get();
	ictl.log = ictl.b->cfg.log;
	ictl.flags = EBLOB_ITERATE_FLAGS_ALL | EBLOB_ITERATE_FLAGS_READONLY;
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
	runnable(cw.config);

	// disable views
	BOOST_TEST_CHECKPOINT("running with views disabled");
	cw.reset_dirs();
	cw.config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS;
	runnable(cw.config);
}

/**
 * 1) Make two bases with 100 records each.
 *    State: data.0(unsorted, 100 records), data.1(unsorted, 100 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 100 records), data.1(unsorted, 100 records)
 * 3) Remove half of first base.
 *    State: data.0(sorted, 50 records), data.1(unsorted, 100 records)
 * 4) Defrag first base.
 *    State: data.0(sorted with view, 50 records), data.1(unsorted, 100 records)
 * 5) Check that bases contains all 150 records
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

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

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
 * 1) Make two bases with 100 records each.
 *    State: data.0(unsorted, 100 records), data.1(unsorted, 100 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 100 records), data.1(unsorted, 100 records)
 * 3) Remove a half from each base
 *    State: data.0(sorted, 50 records), data.1(unsorted, 50 records)
 * 4) Defrag two bases with view on first base
 *    State: data.0(sorted, 100 records)
 * 5) Check that result base contains 100 records
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

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

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
 *  1) Make two bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records)
 *  2) Defrag two bases separately
 *     State: data.0(sorted, 100 records), data.1(sorted, 100 records)
 *  3) Remove a half from each base
 *     State: data.0(sorted, 50 records), data.1(sorted, 50 records)
 *  4) Defrag two bases with view on first base
 *     State: data.0(sorted, 100 records)
 *  5) Check that result base contains 100 records
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

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

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
 *  1) Make two bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records)
 *  3) Remove a half from each base
 *     State: data.0(unsorted, 50 records), data.1(unsorted, 50 records)
 *  4) Defrag two bases without view on bases
 *     State: data.0(sorted, 100 records)
 *  5) Check that result base contains 100 records
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

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

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
 *  1) Make three bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records), data.2(unsorted, 100 records)
 *  3) Remove elements from two bases
 *     State: data.0(unsorted, 0 records), data.1(unsorted, 0 records), data.2(unsorted, 100 records)
 *  4) Defrag all bases
 *     State: data.2(unsorted, 100 records)
 *  5) Check that result base contains 100 records
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

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

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
