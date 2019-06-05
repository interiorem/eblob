#pragma once

#include <boost/filesystem.hpp>

#include <memory>
#include <string>
#include <vector>

#include "eblob/eblob.hpp"
#include "library/blob.h"
#include "library/crypto/sha512.h"

class eblob_config_test_wrapper {
public:
	eblob_config_test_wrapper(bool cleanup_files = true);
	~eblob_config_test_wrapper();

	eblob_config_test_wrapper(const eblob_config_test_wrapper &) = delete;
	eblob_config_test_wrapper &operator=(const eblob_config_test_wrapper &) = delete;

	eblob_config_test_wrapper(eblob_config_test_wrapper &&) = default;
	eblob_config_test_wrapper &operator=(eblob_config_test_wrapper &&) = default;

	void reset_dirs();

	// base directory where backend should store everything
	std::string base_dir() const;

private:
	void cleanup_files();

public:
	eblob_config config;

private:
	bool cleanup_files_;
	std::string data_dir_template_;
	std::string data_dir_;
	std::string data_path_;
	std::string log_path_;
	std::unique_ptr<ioremap::eblob::eblob_logger> logger_;
};

class item_t {
public:

	item_t(uint64_t key_, const eblob_key &hashed_key_, const std::vector<char> &value_);

	bool operator< (const item_t &rhs) const;

	bool checked = false;
	bool removed = false;
	uint64_t key;
	eblob_key hashed_key;

	std::vector<char> value;
};

class eblob_wrapper {
public:
	explicit eblob_wrapper(eblob_config config);

	eblob_wrapper(const eblob_wrapper &) = delete;

	eblob_wrapper &operator=(const eblob_wrapper &) = delete;

	~eblob_wrapper();

	void restart();

	eblob_backend *get();

	const eblob_backend *get() const;

	int insert_item(item_t &item);

	int remove_item(item_t &item);

private:
	eblob_config config_;
	eblob_backend *backend_ = nullptr;
};


constexpr uint64_t DEFAULT_RANDOM_SEED = 42;

template<class D>
class item_generator {
public:

	item_generator(eblob_wrapper &wrapper, D d, uint64_t seed = DEFAULT_RANDOM_SEED)
	: wrapper_(wrapper)
	, gen_(std::mt19937(seed))
	, dist_(std::move(d))
	, data_dist_('a', 'a' + 26) {
	}

	item_t generate_item(uint64_t key) {
		size_t datasize = dist_(gen_);
		std::vector<char> data = generate_random_data(datasize);
		struct eblob_key hashed_key;
		eblob_hash(wrapper_.get(), hashed_key.id, sizeof(hashed_key.id), &key, sizeof(key));
		return item_t(key, hashed_key, data);
	}

private:
	std::vector<char> generate_random_data(size_t datasize) {
		std::vector<char> data(datasize);
		for (auto &element : data) {
			element = data_dist_(gen_);
		}

		return data;
	}
private:
	eblob_wrapper &wrapper_;
	std::mt19937 gen_;
	D dist_;
	std::uniform_int_distribution<char> data_dist_;
};

template<class D>
item_generator<D>
make_item_generator(eblob_wrapper &wrapper, D d, uint64_t seed) {
	return item_generator<D>(wrapper, std::move(d), seed);
}

inline
item_generator<std::uniform_int_distribution<unsigned>>
make_big_item_generator(eblob_wrapper &wrapper, uint64_t seed = DEFAULT_RANDOM_SEED) {
	// uniform distribution from 512KiB to 4MiB
	std::uniform_int_distribution<unsigned> dist(2 << 19, 2 << 22);
	return make_item_generator(wrapper, std::move(dist), seed);
}

inline
item_generator<std::piecewise_constant_distribution<double>>
make_default_item_generator(eblob_wrapper &wrapper, uint64_t seed = DEFAULT_RANDOM_SEED) {
	// 90% for file less than 1KiB
	// 10% for file 2MiB
	std::vector<double> i{0, (1 << 10) + 1,  2 * (1 << 20), 2 * (1 << 20) + 1};
	std::vector<double> w{  9,  0,  1};
	std::piecewise_constant_distribution<> dist(i.begin(), i.end(), w.begin());
	return item_generator<std::piecewise_constant_distribution<double>>(wrapper, dist, seed);
}

eblob_key hash(std::string key);
