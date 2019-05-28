#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE CORRUPTION library test

#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>

#include <future>

#include "eblob_wrapper.h"

#include "library/blob.h"

#include "eblob/eblob.hpp"


BOOST_AUTO_TEST_CASE(test_header_corruption) {
	/* corrupt record's header in blob and check that record isn't considered as corrupted (since data is correct)
	 * and read is failed with -EINVAL
	 */
	eblob_config_test_wrapper config_wrapper;
	eblob_wrapper wrapper(config_wrapper.config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), 0), 0);
	// corrupt header
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, 0), 0);

	// checksum verification should not failed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EINVAL);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), -EINVAL);
	// check that the record was not marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	// restore header
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), 0), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
}

BOOST_AUTO_TEST_CASE(test_data_corruption) {
	/* corrupt record's data and check that the record is considered as corrupted */
	eblob_config_test_wrapper config_wrapper;
	eblob_wrapper wrapper(config_wrapper.config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.index, 0); // in the first blob
	BOOST_REQUIRE_EQUAL(wc.ctl_index_offset, 0); // at the beginning of the index file
	BOOST_REQUIRE_EQUAL(wc.ctl_data_offset, 0); // at the beginning of the blob
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);
	BOOST_REQUIRE_EQUAL(wc.size, sizeof(data));
	BOOST_REQUIRE_EQUAL(wc.total_data_size, sizeof(data));

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), wc.data_offset), 0);
	// corrupt data
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, wc.data_offset), 0);

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), -EILSEQ);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EILSEQ);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), 0);
	// check that the record was marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	// restore data
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), wc.data_offset), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	// TODO(shaitan): maybe we should drop BLOB_DISK_CTL_CORRUPTED if whole record verification has succeeded
	// TODO(shaitan): but we can't drop BLOB_DISK_CTL_CORRUPTED if only part of the record was successfully verified
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
}

BOOST_AUTO_TEST_CASE(test_footer_corruption) {
	/* corrupt record's footer and check that the record is considered as corrupted */
	eblob_config_test_wrapper config_wrapper;
	eblob_wrapper wrapper(config_wrapper.config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);

	auto key = hash("some key");
	constexpr char data[] = "some data";
	constexpr uint64_t MY_DISK_CTL_FLAG = 1 << 30;

	eblob_write_control wc;
	BOOST_REQUIRE_EQUAL(
		eblob_write_return(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), MY_DISK_CTL_FLAG, &wc),
		0);
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM);

	const uint64_t footer_offset = wc.data_offset + wc.total_data_size;

	// read current byte
	char current_byte;
	BOOST_REQUIRE_EQUAL(__eblob_read_ll(wc.data_fd, &current_byte, sizeof(current_byte), footer_offset), 0);
	// corrupt footer
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, footer_offset), 0);

	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), -EILSEQ);
	// verify checksum via read
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EILSEQ);
	// read @wc without checksum verification
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_NOCSUM, &wc), 0);
	// check that the record was marked corrupted
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	// restore footer
	BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, &current_byte, sizeof(current_byte), footer_offset), 0);
	// checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_verify_checksum(wrapper.get(), &key, &wc), 0);
	// read @wc with checksum verification should succeed
	BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
	// the record should still be marked corrupted
	// TODO(shaitan): maybe we should drop BLOB_DISK_CTL_CORRUPTED if whole 1 verification has succeeded
	// TODO(shaitan): but we can't drop BLOB_DISK_CTL_CORRUPTED if only part of the record was successfully verified
	BOOST_REQUIRE_EQUAL(wc.flags, MY_DISK_CTL_FLAG | BLOB_DISK_CTL_CHUNKED_CSUM | BLOB_DISK_CTL_CORRUPTED);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);

	wrapper.restart();

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 1);
	BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
}

BOOST_AUTO_TEST_CASE(test_inspection) {
	eblob_config_test_wrapper config_wrapper;
	eblob_wrapper wrapper(config_wrapper.config);
	BOOST_REQUIRE(wrapper.get() != nullptr);

	constexpr char data[] = "some data";
	constexpr size_t keys_number = 1000;

	for (size_t i = 0; i < keys_number; ++i) {
		auto key = hash(std::to_string(i));
		BOOST_REQUIRE_EQUAL(
			eblob_write(wrapper.get(), &key, (void *)data, /*offset*/ 0, sizeof(data), /*flags*/ 0),
			0
		);
	}

	eblob_write_control wc;
	size_t count = 0;
	for (size_t i = 0; i < keys_number; i += 10) {
		auto key = hash(std::to_string(i));
		BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), 0);
		// corrupt data
		BOOST_REQUIRE_EQUAL(__eblob_write_ll(wc.data_fd, "a", 1, wc.data_offset), 0);
		++count;
	}

	wrapper.get()->want_inspect = EBLOB_INSPECT_STATE_INSPECTING;
	BOOST_REQUIRE_EQUAL(eblob_inspect(wrapper.get()), 0);
	wrapper.get()->want_inspect = EBLOB_INSPECT_STATE_NOT_STARTED;

	wrapper.get()->want_defrag = EBLOB_DEFRAG_STATE_DATA_SORT;
	BOOST_REQUIRE_EQUAL(eblob_defrag(wrapper.get()), 0);
	wrapper.get()->want_defrag = EBLOB_DEFRAG_STATE_NOT_STARTED;

	BOOST_REQUIRE_EQUAL(eblob_periodic(wrapper.get()), 0);

	BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), count);

	{

		// read of corrupted key should not increase the number of corrupted records
		auto key = hash(std::to_string(0));
		BOOST_REQUIRE_EQUAL(eblob_read_return(wrapper.get(), &key, EBLOB_READ_CSUM, &wc), -EILSEQ);
		BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), count);
	}

	{
		// remove of corrupted key should decrease the number of corrupted records
		auto key = hash(std::to_string(0));
		BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
		BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), --count);
	}

	{
		// rewrite of corrupted key should decrease the number of corrupted records
		auto key = hash(std::to_string(10));
		constexpr char new_data[] = "some new data";
		BOOST_REQUIRE_EQUAL(
			eblob_write(wrapper.get(), &key, (void *)new_data, /*offset*/ 0, sizeof(new_data), /*flags*/ 0),
			0
		);
		BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), --count);
	}

	{
		//remove all corrupted key should set corrupted records and size to 0
		for (size_t i = 20; i < keys_number; i += 10) {
			auto key = hash(std::to_string(i));
			BOOST_REQUIRE_EQUAL(eblob_remove(wrapper.get(), &key), 0);
			BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED),
			                    --count);
		}

		BOOST_REQUIRE_EQUAL(count, 0);
		BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_RECORDS_CORRUPTED), 0);
		BOOST_REQUIRE_EQUAL(eblob_stat_get(wrapper.get()->stat_summary, EBLOB_LST_CORRUPTED_SIZE), 0);

	}
}
