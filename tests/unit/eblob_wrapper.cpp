#include "eblob_wrapper.h"

#include <boost/filesystem.hpp>

#include <library/blob.h>
#include <library/crypto/sha512.h>

#include <eblob/eblob.hpp>

#include <vector>


eblob_config_test_wrapper::eblob_config_test_wrapper(bool cleanup_files)
: cleanup_files_(cleanup_files) {
	reset_dirs();
	config.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS | EBLOB_AUTO_INDEXSORT;
	config.sync = -2;
	config.blob_size = EBLOB_BLOB_DEFAULT_BLOB_SIZE;
	config.records_in_blob = EBLOB_BLOB_DEFAULT_RECORDS_IN_BLOB;
	config.defrag_percentage = EBLOB_DEFAULT_DEFRAG_PERCENTAGE;
	config.defrag_timeout = EBLOB_DEFAULT_DEFRAG_TIMEOUT;
	config.index_block_size = EBLOB_INDEX_DEFAULT_BLOCK_SIZE;
	config.index_block_bloom_length = EBLOB_INDEX_DEFAULT_BLOCK_BLOOM_LENGTH;
	config.blob_size_limit = UINT64_MAX;
	config.defrag_time = EBLOB_DEFAULT_DEFRAG_TIME;
	config.defrag_splay = EBLOB_DEFAULT_DEFRAG_SPLAY;
	config.periodic_timeout = EBLOB_DEFAULT_PERIODIC_THREAD_TIMEOUT;
	config.stat_id = 12345;
}

eblob_config_test_wrapper::~eblob_config_test_wrapper() {
	cleanup_files();
}

void eblob_config_test_wrapper::cleanup_files() {
	if (cleanup_files_ && !data_dir_.empty()) {
		boost::filesystem::remove_all(data_dir_);
	}
}

void eblob_config_test_wrapper::reset_dirs() {
	cleanup_files();
	data_dir_template_ = "/tmp/eblob-test-XXXXXX";
	data_dir_ = mkdtemp(&data_dir_template_.front());
	data_path_ = data_dir_ + "/data";
	log_path_ = data_dir_ + "/log.log";
	logger_.reset(new ioremap::eblob::eblob_logger(log_path_.c_str(), EBLOB_LOG_DEBUG));
	config.log = logger_->log();
	config.file = const_cast<char*>(data_path_.c_str());
	config.chunks_dir = const_cast<char*>(data_dir_.c_str());
}

item_t::item_t(uint64_t key_, const eblob_key &hashed_key_, const std::vector<char> &value_)
: key(key_)
, value(value_) {
	memcpy(hashed_key.id, hashed_key_.id, EBLOB_ID_SIZE);
}


bool item_t::operator< (const item_t &rhs) const {
	return eblob_id_cmp(hashed_key.id, rhs.hashed_key.id) < 0;
}


eblob_wrapper::eblob_wrapper(eblob_config config)
: config_(config) {
	backend_ = eblob_init(&config_);
}

eblob_wrapper::~eblob_wrapper() {
	eblob_cleanup(backend_);
}

void eblob_wrapper::restart() {
	eblob_cleanup(backend_);
	backend_ = eblob_init(&config_);
}

eblob_backend *eblob_wrapper::get() {
	return backend_;
}


const eblob_backend *eblob_wrapper::get() const {
	return backend_;
}


int eblob_wrapper::insert_item(item_t &item) {
	return eblob_write(get(), &item.hashed_key, item.value.data(), /* offset */ 0, item.value.size(), /*flags*/ 0);
}


int eblob_wrapper::remove_item(item_t &item) {
	item.removed = true;
	return eblob_remove_hashed(get(), &item.key, sizeof(item.key));
}

eblob_key hash(std::string key) {
	eblob_key ret;
	sha512_buffer(key.data(), key.size(), ret.id);
	return ret;
}
