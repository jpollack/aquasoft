#pragma once

#include <string>

// Flags
#define AS_MSG_FLAG_READ                   (1 << 0) // contains a read operation
#define AS_MSG_FLAG_GET_ALL                (1 << 1) // get all bins
#define AS_MSG_FLAG_SHORT_QUERY            (1 << 2) // bypass monitoring, inline if data-in-memory
#define AS_MSG_FLAG_BATCH                  (1 << 3) // batch protocol
#define AS_MSG_FLAG_XDR                    (1 << 4) // operation is via XDR
#define AS_MSG_FLAG_GET_NO_BINS            (1 << 5) // get record metadata only - no bin metadata or data
#define AS_MSG_FLAG_CONSISTENCY_LEVEL_ALL  (1 << 6) // duplicate resolve reads
#define AS_MSG_FLAG_COMPRESS_RESPONSE      (1 << 7) // (enterprise only)
#define AS_MSG_FLAG_WRITE                  (1 << 8) // contains a write semantic
#define AS_MSG_FLAG_DELETE                 (1 << 9) // delete record
#define AS_MSG_FLAG_GENERATION             (1 << 10) // pay attention to the generation
#define AS_MSG_FLAG_GENERATION_GT          (1 << 11) // apply write if new generation > old, good for restore
#define AS_MSG_FLAG_DURABLE_DELETE         (1 << 12) // op resulting in record deletion leaves tombstone (enterprise only)
#define AS_MSG_FLAG_CREATE_ONLY            (1 << 13) // write record only if it doesn't exist
#define AS_MSG_FLAG_RELAX_AP_LONG_QUERY    (1 << 14) // AP long queries reserve partitions as long as they have data
#define AS_MSG_FLAG_RESPOND_ALL_OPS        (1 << 15) // all bin ops (read, write, or modify) require a response, in request order
#define AS_MSG_FLAG_LAST                   (1 << 16) // this is the last of a multi-part message
#define AS_MSG_FLAG_COMMIT_LEVEL_MASTER    (1 << 17) // "fire and forget" replica writes
#define AS_MSG_FLAG_PARTITION_DONE         (1 << 18) // in query response, partition is done
#define AS_MSG_FLAG_UPDATE_ONLY            (1 << 19) // update existing record only, do not create new record
#define AS_MSG_FLAG_CREATE_OR_REPLACE      (1 << 20) // completely replace existing record, or create new record
#define AS_MSG_FLAG_REPLACE_ONLY           (1 << 21) // completely replace existing record, do not create new record
#define AS_MSG_FLAG_SC_READ_TYPE           (1 << 22) // (enterprise only)
#define AS_MSG_FLAG_SC_READ_RELAX          (1 << 23) // (enterprise only)
#define AS_MSG_FLAG_MRT_VERIFY_READ		(1 << 24)
#define AS_MSG_FLAG_MRT_ROLL_FORWARD		(1 << 25)
#define AS_MSG_FLAG_MRT_ROLL_BACK		(1 << 26)
#define AS_MSG_FLAG_MRT_MONITOR_DRIVEN		(1 << 27)
#define AS_MSG_FLAG_MRT_UNLOCKED_ONLY		(1 << 28)
#define MONITOR_SET_NAME "<ERO~MRT"

struct as_header;
struct as_msgpkt;
struct as_msg;

struct as_header
{
    uint8_t version;
    uint8_t type;
    uint16_t be_sz_extra;
    uint32_t be_sz;
    as_header (uint8_t _type = 1, size_t _size = 0);
    as_header (const as_msg* msg);
    as_header (const std::string& str);
    as_header* init (const as_msg* msg);
    as_header* init (const std::string & str);
    size_t size (void) const;
    void size (size_t sz);
} __attribute__((__packed__));


struct as_particle
{
    enum class type : uint8_t
    {
	t_null =	0,		// AS_BYTES_UNDEF
	t_integer =	1,		// AS_BYTES_INTEGER
	t_float =	2,		// AS_BYTES_DOUBLE
	t_string =	3,		// AS_BYTES_STRING
	t_blob =	4,		// AS_BYTES_BLOB
	t_boolean =	17,
	t_hll =		18,
	t_map =		19,
	t_list =	20,
	t_geojson =	23
    };
    as_particle::type t;
    uint8_t data[0];
}__attribute__((__packed__));

struct as_field
{
    enum class type : uint8_t
    {
	t_namespace =		0,
	t_set =			1,
	t_key =			2,
	t_record_version =	3,	// 7 bytes
	t_digest_ripe =		4,	// 20 bytes
	t_mrtid =		5,	// 8 bytes little endian
	t_mrt_deadline =	6,	// 4 bytes little endian
	t_trid =		7,
	t_socket_timeout =	9,
	t_recs_per_sec =	10,
	t_pid_array =		11,
	t_digest_array =	12,
	t_sample_max =		13,	// 8 bytes big endian
	t_lut =			14, // for xdr writes only
	t_bval_array =		15,
	t_index_name =		21, // was superfluous - but reserved for future use
	t_index_range =		22,
	t_index_context =	23,
	t_index_type =		26,
	t_udf_filename =	30,
	t_udf_function =	31,
	t_udf_arglist =		32,
	t_udf_op =		33,		// 1 byte
	t_query_binlist =	40, // deprecated - now use bin-ops
	t_batch =		41,
	t_batch_with_set =	42,
	t_predexp =		43
    };
    uint32_t be_sz;
    as_field::type t;
    uint8_t data[0];
    size_t data_sz (void);
    as_field *next (void);
} __attribute__((__packed__));

struct as_op
{
    enum class type : uint8_t
    {
	t_none =	0,
	t_read =	1,
	t_write =	2,
	t_cdt_read =	3,
	t_cdt_modify =	4,
	t_incr =	5,
	t_exp_read =	7,
	t_exp_modify =	8,
	t_append =	9,
	t_prepend =	10,
	t_touch =	11,
	t_bits_read =	12,
	t_bits_modify =	13,
	t_delete_all =	14,
	t_hll_read =	15,
	t_hll_modify =	16
    };

    uint32_t be_sz;
    as_op::type op_type;
    as_particle::type data_type;
    uint8_t flags;
    uint8_t name_sz;
    uint8_t name[0];
    uint8_t *data (void);  // { return this->name + this->name_sz; }
    size_t data_sz (void) const;
    void data_sz (size_t sz);
    as_op *next (void);
} __attribute__((__packed__));

struct as_msg
{
    uint8_t _res0; // size of this header (always 22)
    uint32_t flags;
    uint8_t result_code;
    uint32_t be_generation;
    uint32_t be_record_ttl;
    uint32_t be_transaction_ttl;
    uint16_t be_fields;
    uint16_t be_ops;
    uint8_t data[0];
    void clear (void);
    as_field *field (as_field::type t) const;
    as_op* ops_begin (void) const;
    uint8_t *end (void) const;
    uint16_t n_ops (void) const;
    uint16_t n_fields (void) const;
    as_field *add (as_field::type t, size_t sz);
    as_field *add (as_field::type t, size_t sz, const void *data);
    as_field *add (as_field::type t, const std::string& str);
    as_op *add (as_op::type t, size_t name_sz, size_t data_sz);
    as_op *add (as_op::type t, const std::string& name, size_t data_sz, as_particle::type dt = as_particle::type::t_blob);
    as_op *add (as_op::type t, const std::string& name, size_t data_sz, const void *data, as_particle::type dt = as_particle::type::t_blob);
    as_op *add (as_op::type t, const std::string& name, const std::string& val);
} __attribute__((__packed__));

size_t write (int fd, const std::string& str);
size_t write (int fd, const as_msg* msg);
size_t read (int fd, void **obuf);
size_t read (int fd, std::string& str);

size_t call (int fd, void **obuf, const as_msg* msg, uint32_t *dur = nullptr);
size_t call (int fd, as_msg **obuf, const as_msg* msg, uint32_t *dur = nullptr);
size_t call (int fd, void **obuf, const std::string& str, uint32_t *dur = nullptr);
size_t call_info (int fd, std::string& obuf, const std::string& ibuf, uint32_t *dur = nullptr);
std::string call_info (int fd, const std::string& str, uint32_t *dur = nullptr);

std::string to_string (const as_field::type t);
std::string to_string (const as_op::type t);
