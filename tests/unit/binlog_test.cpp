#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE BINLOG library test

#include <boost/test/included/unit_test.hpp>
#include <boost/test/unit_test.hpp>

#include <vector>

#include "eblob_wrapper.h"

#include "library/datasort.h"


eblob_config_test_wrapper initialize_eblob_config_for_binlog() {
	eblob_config_test_wrapper config_wrapper;
	eblob_config &config = config_wrapper.config;
	constexpr size_t RECORDS_IN_BLOB = 10;
	config.records_in_blob = RECORDS_IN_BLOB;
	return config_wrapper;
}

/**
 * Make a blob with 10 records 100 length each.
 * Invoke write_prepare with key in existing base and smaller size.
 * Check that number of bases wasn't changed
 */
BOOST_AUTO_TEST_CASE(write_prepare_without_binlog) {
	eblob_config_test_wrapper config_wrapper = initialize_eblob_config_for_binlog();
	eblob_config &config = config_wrapper.config;
	size_t TOTAL_RECORDS = config.records_in_blob;

	eblob_wrapper wrapper(config);

	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 100, 100);
	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);

	BOOST_REQUIRE_EQUAL(eblob_write_prepare(wrapper.get(), &shadow_elems.back().hashed_key, 50, 0), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);
}


/**
 * Make a blob with 10 records 100 length each.
 * Invoke write_prepare with absent key.
 * Check that number of bases changed
 */
BOOST_AUTO_TEST_CASE(write_prepare_without_binlog2) {
	eblob_config_test_wrapper config_wrapper = initialize_eblob_config_for_binlog();
	eblob_config &config = config_wrapper.config;
	size_t TOTAL_RECORDS = config.records_in_blob;

	eblob_wrapper wrapper(config);

	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 100, 100);
	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);

	item_t new_item = generator.generate_sized_item(1000, 50);
	BOOST_REQUIRE_EQUAL(eblob_write_prepare(wrapper.get(), &new_item.hashed_key, 50, 0), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 2);
}


/**
 * Make a blob with 10 records 100 length each.
 * Invoke write_prepare with key in existing base and bigger size.
 * Check that number of bases changed
 */
BOOST_AUTO_TEST_CASE(write_prepare_without_binlog3) {
	eblob_config_test_wrapper config_wrapper = initialize_eblob_config_for_binlog();
	eblob_config &config = config_wrapper.config;
	size_t TOTAL_RECORDS = config.records_in_blob;

	eblob_wrapper wrapper(config);

	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 100, 100);
	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);

	BOOST_REQUIRE_EQUAL(eblob_write_prepare(wrapper.get(), &shadow_elems.back().hashed_key, 150, 0), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 2);
}


/**
 * Make a blob with 10 records 100 length each.
 * Invoke write_prepare with key in existing base and size equal to the previous one.
 * Check that number of bases wasn't changed
 */
BOOST_AUTO_TEST_CASE(write_prepare_without_binlog4) {
	eblob_config_test_wrapper config_wrapper = initialize_eblob_config_for_binlog();
	eblob_config &config = config_wrapper.config;
	size_t TOTAL_RECORDS = config.records_in_blob;

	eblob_wrapper wrapper(config);

	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 100, 100);
	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);

	BOOST_REQUIRE_EQUAL(eblob_write_prepare(wrapper.get(), &shadow_elems.back().hashed_key, 100, 0), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);
}


/**
 * Make a blob with 10 records 100 length each.
 * Invoke write_prepare with key in existing base and smaller size and enabled binlog.
 * Check that number of bases changed
 */
BOOST_AUTO_TEST_CASE(write_prepare_with_binlog) {
	eblob_config_test_wrapper config_wrapper = initialize_eblob_config_for_binlog();
	eblob_config &config = config_wrapper.config;
	size_t TOTAL_RECORDS = config.records_in_blob;

	eblob_wrapper wrapper(config);

	std::vector<item_t> shadow_elems;
	auto generator = make_uniform_item_generator(wrapper, 100, 100);
	BOOST_REQUIRE_EQUAL(fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 1);

	eblob_base_ctl *bctl = container_of(wrapper.get()->bases.next, eblob_base_ctl, base_entry);
	BOOST_REQUIRE_EQUAL(eblob_binlog_start(&bctl->binlog), 0);
	BOOST_REQUIRE_EQUAL(eblob_write_prepare(wrapper.get(), &shadow_elems.back().hashed_key, 50, 0), 0);
	BOOST_REQUIRE_EQUAL(wrapper.number_bases(), 2);
}
