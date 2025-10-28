#pragma once

#include <string>
#include <nlohmann/json.hpp>

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
	t_index_expression =	24,
	t_index_type =		26,
	t_udf_filename =	30,
	t_udf_function =	31,
	t_udf_arglist =		32,
	t_udf_op =		33,		// 1 byte
	t_query_binlist =	40, // deprecated - now use bin-ops
	t_batch =		41,
	t_batch_with_set =	42,
	t_predexp =		43,
	t_conndata =		50
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
    as_field *add (as_field::type t, const nlohmann::json& data);
    as_op *add (as_op::type t, size_t name_sz, size_t data_sz);
    as_op *add (as_op::type t, const std::string& name, size_t data_sz, as_particle::type dt = as_particle::type::t_blob);
    as_op *add (as_op::type t, const std::string& name, size_t data_sz, const void *data, as_particle::type dt = as_particle::type::t_blob);
    as_op *add (as_op::type t, const std::string& name, const std::string& val);
    as_op *add (as_op::type t, const std::string& name, const nlohmann::json& data);
} __attribute__((__packed__));

// Expression opcodes
struct as_exp
{
    enum class op : int
    {
	// Comparison
	cmp_eq =	1,
	cmp_ne =	2,
	cmp_gt =	3,
	cmp_ge =	4,
	cmp_lt =	5,
	cmp_le =	6,
	cmp_regex =	7,
	cmp_geo =	8,
	// Logical
	and_ =		16,
	or_ =		17,
	not_ =		18,
	exclusive =	19,
	// Arithmetic
	add =		20,
	sub =		21,
	mul =		22,
	div =		23,
	pow =		24,
	log =		25,
	mod =		26,
	abs =		27,
	floor =		28,
	ceil =		29,
	// Type conversion
	to_int =	30,
	to_float =	31,
	// Bitwise
	int_and =	32,
	int_or =	33,
	int_xor =	34,
	int_not =	35,
	int_lshift =	36,
	int_rshift =	37,
	int_arshift =	38,
	int_count =	39,
	int_lscan =	40,
	int_rscan =	41,
	// Min/Max
	min =		50,
	max =		51,
	// Metadata
	meta_digest_mod =	64,
	meta_device_size =	65,	// deprecated
	meta_last_update =	66,
	meta_since_update =	67,
	meta_void_time =	68,
	meta_ttl =		69,
	meta_set_name =		70,
	meta_key_exists =	71,
	meta_is_tombstone =	72,
	meta_memory_size =	73,	// deprecated
	meta_record_size =	74,
	// Record/Bin
	rec_key =	80,
	bin =		81,
	bin_type =	82,
	// Control flow
	result_remove =	100,
	var_builtin =	122,
	cond =		123,
	var =		124,
	let =		125,
	quote =		126,
	call =		127
    };

    enum class result_type : int
    {
	t_nil =		0,
	t_bool =	1,
	t_int =		2,
	t_str =		3,
	t_list =	4,
	t_map =		5,
	t_blob =	6,
	t_float =	7,
	t_geojson =	8,
	t_hll =		9
    };

    enum class flags : int
    {
	none =         0,       // Default - no special flags
	create_only =  1 << 0,  // Only execute if record doesn't exist
	update_only =  1 << 1,  // Only execute if record exists
	allow_delete = 1 << 2,  // Allow expression to delete the bin
	policy_no_fail = 1 << 3, // Transaction does not fail if expression false
	eval_no_fail = 1 << 4   // Expression evaluation errors don't fail transaction
    };
};

// CDT opcodes
struct as_cdt
{
    enum class list_op : int
    {
	set_type =			0,
	append =			1,
	append_items =			2,
	insert =			3,
	insert_items =			4,
	pop =				5,
	pop_range =			6,
	remove =			7,
	remove_range =			8,
	set =				9,
	trim =				10,
	clear =				11,
	increment =			12,
	sort =				13,
	size =				16,
	get =				17,
	get_range =			18,
	get_by_index =			19,
	get_by_value =			20,
	get_by_rank =			21,
	get_all_by_value =		22,
	get_all_by_value_list =		23,
	get_by_index_range =		24,
	get_by_value_interval =		25,
	get_by_rank_range =		26,
	get_by_value_rel_rank_range =	27,
	remove_by_index =		32,
	remove_by_value =		33,
	remove_by_rank =		34,
	remove_all_by_value =		35,
	remove_all_by_value_list =	36,
	remove_by_index_range =		37,
	remove_by_value_interval =	38,
	remove_by_rank_range =		39,
	remove_by_value_rel_rank_range = 40
    };

    enum class map_op : int
    {
	set_type =			64,
	add =				65,
	add_items =			66,
	put =				67,
	put_items =			68,
	replace =			69,
	replace_items =			70,
	increment =			73,
	decrement =			74,
	clear =				75,
	remove_by_key =			76,
	remove_by_index =		77,
	remove_by_value =		78,
	remove_by_rank =		79,
	remove_by_key_list =		81,
	remove_all_by_value =		82,
	remove_by_value_list =		83,
	remove_by_key_interval =	84,
	remove_by_index_range =		85,
	remove_by_value_interval =	86,
	remove_by_rank_range =		87,
	remove_by_key_rel_index_range =	88,
	remove_by_value_rel_rank_range = 89,
	size =				96,
	get_by_key =			97,
	get_by_index =			98,
	get_by_value =			99,
	get_by_rank =			100,
	get_all_by_value =		102,
	get_by_key_interval =		103,
	get_by_index_range =		104,
	get_by_value_interval =		105,
	get_by_rank_range =		106,
	get_by_key_list =		107,
	get_by_value_list =		108,
	get_by_key_rel_index_range =	109,
	get_by_value_rel_rank_range =	110
    };

    // Special CDT operations (work on both lists and maps)
    enum class special_op : int
    {
	select =	    254,  // 0xFE - CDT SELECT (expression-based filtering)
	subcontext_eval =   255   // 0xFF - Subcontext evaluation (nested operations)
    };

    enum class return_type : int
    {
	none =		0,  // Don't return anything
	index =		1,  // Return index
	reverse_index =	2,  // Return reverse index
	rank =		3,  // Return rank
	reverse_rank =	4,  // Return reverse rank
	count =		5,  // Return count
	key =		6,  // Return key (map only)
	value =		7,  // Return value (default for reads)
	map =		8,  // Return map
	inverted =	16, // Inverted flag (bitmap)
    };

    // CDT context types for nested operations
    enum class ctx_type : int
    {
	exp =		 0x04,  // AS_CDT_CTX_EXP - Expression-based context filter
	list_index = 0x10,  // Navigate to list element by index
	list_rank =  0x11,  // Navigate to list element by rank
	list_value = 0x13,  // Navigate to list element by value
	map_index =  0x20,  // Navigate to map pair by index
	map_rank =   0x21,  // Navigate to map pair by rank
	map_key =    0x22,  // Navigate to map pair by key
	map_value =  0x23,  // Navigate to map pair by value
    };

    // CDT context creation flags (for nested modify operations)
    enum class ctx_create : int
    {
	// List creation types
	list_unordered =        0x40,
	list_unordered_unbound = 0x80,
	list_ordered =          0xc0,
	// Map creation types
	map_unordered =  0x40,
	map_k_ordered =  0x80,
	map_kv_ordered = 0xc0,
	// Persist index flag
	persist_index =  0x100,
    };

    // List ordering flags
    enum class list_order : int
    {
	unordered = 0x00,  // AS_PACKED_LIST_FLAG_NONE
	ordered   = 0x01   // AS_PACKED_LIST_FLAG_ORDERED
    };

    // Map ordering flags
    enum class map_order : int
    {
	unordered  = 0x00,  // AS_PACKED_MAP_FLAG_NONE
	k_ordered  = 0x01,  // AS_PACKED_MAP_FLAG_K_ORDERED (keys sorted)
	v_ordered  = 0x02,  // AS_PACKED_MAP_FLAG_V_ORDERED (values sorted, requires k_ordered)
	kv_ordered = 0x03   // AS_PACKED_MAP_FLAG_KV_ORDERED (key-value multi-map)
    };

    // List write flags (from server cdt.c)
    enum class list_write_flags : int
    {
	default_       = 0,  // No special behavior
	add_unique     = 1,  // AS_CDT_LIST_ADD_UNIQUE - Fail if element exists
	insert_bounded = 2,  // AS_CDT_LIST_INSERT_BOUNDED - Fail if index out of bounds
	no_fail        = 4,  // AS_CDT_LIST_NO_FAIL - Don't fail on parameter errors
	do_partial     = 8   // AS_CDT_LIST_DO_PARTIAL - Partial success on multi-element ops
    };

    // Map write flags (from server cdt.c)
    enum class map_write_flags : int
    {
	default_     = 0,  // No special behavior
	create_only  = 1,  // AS_CDT_MAP_CREATE_ONLY - Fail if key exists (like ADD)
	update_only  = 2,  // AS_CDT_MAP_UPDATE_ONLY - Fail if key doesn't exist
	no_fail      = 4,  // AS_CDT_MAP_NO_FAIL - Don't fail on parameter errors
	do_partial   = 8   // AS_CDT_MAP_DO_PARTIAL - Partial success on multi-item ops
    };

    // Built-in variables for CDT SELECT expression evaluation
    // These variables are available during SELECT expression evaluation
    // and reference different aspects of the current element being evaluated
    enum class builtin_var : uint8_t
    {
	key = 0,    // AS_EXP_BUILTIN_KEY - Map key (available in map contexts only)
	value = 1,  // AS_EXP_BUILTIN_VALUE - Element value (lists) or entry value (maps)
	index = 2   // AS_EXP_BUILTIN_INDEX - Element index (0-based position)
    };
};

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
std::string to_string (const as_exp::op t);
std::string to_string (const as_exp::result_type t);
std::string to_string (const as_cdt::list_op t);
std::string to_string (const as_cdt::map_op t);

// Expression helper functions
namespace expr
{
    using json = nlohmann::json;

    // Comparison operations
    inline json eq  (json a, json b) { return {as_exp::op::cmp_eq, a, b}; }
    inline json ne  (json a, json b) { return {as_exp::op::cmp_ne, a, b}; }
    inline json gt  (json a, json b) { return {as_exp::op::cmp_gt, a, b}; }
    inline json ge  (json a, json b) { return {as_exp::op::cmp_ge, a, b}; }
    inline json lt  (json a, json b) { return {as_exp::op::cmp_lt, a, b}; }
    inline json le  (json a, json b) { return {as_exp::op::cmp_le, a, b}; }
    inline json regex(json a, json b) { return {as_exp::op::cmp_regex, a, b}; }
    inline json geo (json a, json b) { return {as_exp::op::cmp_geo, a, b}; }

    // Logical operations
    inline json and_(json a, json b) { return {as_exp::op::and_, a, b}; }
    inline json or_ (json a, json b) { return {as_exp::op::or_, a, b}; }
    inline json not_(json a)         { return {as_exp::op::not_, a}; }
    inline json exclusive(json a, json b) { return {as_exp::op::exclusive, a, b}; }

    // Arithmetic operations
    inline json add  (json a, json b) { return {as_exp::op::add, a, b}; }
    inline json sub  (json a, json b) { return {as_exp::op::sub, a, b}; }
    inline json mul  (json a, json b) { return {as_exp::op::mul, a, b}; }
    inline json div  (json a, json b) { return {as_exp::op::div, a, b}; }
    inline json pow  (json a, json b) { return {as_exp::op::pow, a, b}; }
    inline json log  (json a, json b) { return {as_exp::op::log, a, b}; }
    inline json mod  (json a, json b) { return {as_exp::op::mod, a, b}; }
    inline json abs  (json a)         { return {as_exp::op::abs, a}; }
    inline json floor(json a)         { return {as_exp::op::floor, a}; }
    inline json ceil (json a)         { return {as_exp::op::ceil, a}; }

    // Type conversion
    inline json to_int  (json a) { return {as_exp::op::to_int, a}; }
    inline json to_float(json a) { return {as_exp::op::to_float, a}; }

    // Bitwise operations
    inline json int_and    (json a, json b) { return {as_exp::op::int_and, a, b}; }
    inline json int_or     (json a, json b) { return {as_exp::op::int_or, a, b}; }
    inline json int_xor    (json a, json b) { return {as_exp::op::int_xor, a, b}; }
    inline json int_not    (json a)         { return {as_exp::op::int_not, a}; }
    inline json int_lshift (json a, json b) { return {as_exp::op::int_lshift, a, b}; }
    inline json int_rshift (json a, json b) { return {as_exp::op::int_rshift, a, b}; }
    inline json int_arshift(json a, json b) { return {as_exp::op::int_arshift, a, b}; }
    inline json int_count  (json a)         { return {as_exp::op::int_count, a}; }
    inline json int_lscan  (json a, json b) { return {as_exp::op::int_lscan, a, b}; }
    inline json int_rscan  (json a, json b) { return {as_exp::op::int_rscan, a, b}; }

    // Min/Max
    inline json min(json a, json b) { return {as_exp::op::min, a, b}; }
    inline json max(json a, json b) { return {as_exp::op::max, a, b}; }

    // Metadata operations
    inline json digest_mod(int mod_value)    { return {as_exp::op::meta_digest_mod, mod_value}; }
    inline json last_update()   { return {as_exp::op::meta_last_update}; }
    inline json since_update()  { return {as_exp::op::meta_since_update}; }
    inline json void_time()     { return {as_exp::op::meta_void_time}; }
    inline json ttl()           { return {as_exp::op::meta_ttl}; }
    inline json set_name()      { return {as_exp::op::meta_set_name}; }
    inline json key_exists()    { return {as_exp::op::meta_key_exists}; }
    inline json is_tombstone()  { return {as_exp::op::meta_is_tombstone}; }
    inline json record_size()   { return {as_exp::op::meta_record_size}; }

    // Record/Bin operations
    inline json rec_key(as_exp::result_type type = as_exp::result_type::t_int) { return {as_exp::op::rec_key, type}; }
    inline json bin(const std::string& name, as_exp::result_type type = as_exp::result_type::t_int) {
        return {as_exp::op::bin, type, name};
    }
    inline json bin_type(const std::string& name) {
        return {as_exp::op::bin_type, name};
    }

    // Built-in variables for CDT SELECT operations
    // Variable IDs: AS_EXP_BUILTIN_KEY=0, AS_EXP_BUILTIN_VALUE=1, AS_EXP_BUILTIN_INDEX=2
    inline json var_builtin_map(as_cdt::builtin_var var) {
        return {as_exp::op::var_builtin, as_exp::result_type::t_map, static_cast<int>(var)};
    }
    inline json var_builtin_list(as_cdt::builtin_var var) {
        return {as_exp::op::var_builtin, as_exp::result_type::t_list, static_cast<int>(var)};
    }
    inline json var_builtin_str(as_cdt::builtin_var var) {
        return {as_exp::op::var_builtin, as_exp::result_type::t_str, static_cast<int>(var)};
    }
    inline json var_builtin_int(as_cdt::builtin_var var) {
        return {as_exp::op::var_builtin, as_exp::result_type::t_int, static_cast<int>(var)};
    }
    inline json var_builtin_float(as_cdt::builtin_var var) {
        return {as_exp::op::var_builtin, as_exp::result_type::t_float, static_cast<int>(var)};
    }

    // Control flow
    inline json cond(json predicate, json true_expr, json false_expr) {
        return {as_exp::op::cond, predicate, true_expr, false_expr};
    }
}

// CDT helper functions
namespace cdt
{
    using json = nlohmann::json;

    // Context builders for nested operations
    namespace ctx
    {
        // List context navigation
        inline json list_index(int index) { return {as_cdt::ctx_type::list_index, index}; }
        inline json list_rank(int rank) { return {as_cdt::ctx_type::list_rank, rank}; }
        inline json list_value(json value) { return {as_cdt::ctx_type::list_value, value}; }

        // Map context navigation
        inline json map_index(int index) { return {as_cdt::ctx_type::map_index, index}; }
        inline json map_rank(int rank) { return {as_cdt::ctx_type::map_rank, rank}; }
        inline json map_key(json key) { return {as_cdt::ctx_type::map_key, key}; }
        inline json map_value(json value) { return {as_cdt::ctx_type::map_value, value}; }
    }

    namespace list
    {
        // Simple operations (no parameters beyond opcode)
        inline json size()  { return {as_cdt::list_op::size}; }
        inline json clear() { return {as_cdt::list_op::clear}; }
        inline json sort()  { return {as_cdt::list_op::sort}; }

        // Modify operations
        inline json set_type(as_cdt::list_order order) { return {as_cdt::list_op::set_type, order}; }
        inline json append(json value) { return {as_cdt::list_op::append, value}; }
        inline json append_items(json list) { return {as_cdt::list_op::append_items, list}; }
        inline json insert(json index, json value) { return {as_cdt::list_op::insert, index, value}; }
        inline json insert_items(json index, json list) { return {as_cdt::list_op::insert_items, index, list}; }
        inline json set(json index, json value) { return {as_cdt::list_op::set, index, value}; }
        inline json trim(json index, json count) { return {as_cdt::list_op::trim, index, count}; }
        inline json increment(json index, json delta) { return {as_cdt::list_op::increment, index, delta}; }

        // Pop operations (modify + return)
        inline json pop(json index) { return {as_cdt::list_op::pop, index}; }
        inline json pop_range(json index, json count) { return {as_cdt::list_op::pop_range, index, count}; }

        // Remove operations
        inline json remove(json index) { return {as_cdt::list_op::remove, index}; }
        inline json remove_range(json index, json count) { return {as_cdt::list_op::remove_range, index, count}; }
        inline json remove_by_index(json index, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_index, rt, index}; }
        inline json remove_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_value, rt, value}; }
        inline json remove_by_rank(json rank, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_rank, rt, rank}; }
        inline json remove_all_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_all_by_value, rt, value}; }
        inline json remove_all_by_value_list(json values, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_all_by_value_list, rt, values}; }
        inline json remove_by_index_range(json index, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_index_range, rt, index, count}; }
        inline json remove_by_value_interval(json value_start, json value_end, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_value_interval, rt, value_start, value_end}; }
        inline json remove_by_rank_range(json rank, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_rank_range, rt, rank, count}; }
        inline json remove_by_value_rel_rank_range(json value, json rank, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::list_op::remove_by_value_rel_rank_range, rt, value, rank, count}; }

        // Read operations
        inline json get(json index) { return {as_cdt::list_op::get, index}; }
        inline json get_range(json index, json count) { return {as_cdt::list_op::get_range, index, count}; }

        // Get by index/value/rank operations
        inline json get_by_index(json index, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_index, rt, index}; }
        inline json get_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_value, rt, value}; }
        inline json get_by_rank(json rank, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_rank, rt, rank}; }
        inline json get_all_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_all_by_value, rt, value}; }
        inline json get_all_by_value_list(json values, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_all_by_value_list, rt, values}; }
        inline json get_by_index_range(json index, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_index_range, rt, index, count}; }
        inline json get_by_value_interval(json value_start, json value_end, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_value_interval, rt, value_start, value_end}; }
        inline json get_by_rank_range(json rank, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_rank_range, rt, rank, count}; }
        inline json get_by_value_rel_rank_range(json value, json rank, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::list_op::get_by_value_rel_rank_range, rt, value, rank, count}; }
    }

    namespace map
    {
        // Simple operations
        inline json size()  { return {as_cdt::map_op::size}; }
        inline json clear() { return {as_cdt::map_op::clear}; }

        // Modify operations
        inline json set_type(as_cdt::map_order order) { return {as_cdt::map_op::set_type, order}; }
        inline json add(json key, json value) { return {as_cdt::map_op::add, key, value}; }
        inline json add_items(json map) { return {as_cdt::map_op::add_items, map}; }
        inline json put(json key, json value) { return {as_cdt::map_op::put, key, value}; }
        inline json put_items(json map) { return {as_cdt::map_op::put_items, map}; }
        inline json replace(json key, json value) { return {as_cdt::map_op::replace, key, value}; }
        inline json replace_items(json map) { return {as_cdt::map_op::replace_items, map}; }
        inline json increment(json key, json delta) { return {as_cdt::map_op::increment, key, delta}; }
        inline json decrement(json key, json delta) { return {as_cdt::map_op::decrement, key, delta}; }

        // Remove operations
        inline json remove_by_key(json key, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_key, rt, key}; }
        inline json remove_by_index(json index, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_index, rt, index}; }
        inline json remove_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_value, rt, value}; }
        inline json remove_by_rank(json rank, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_rank, rt, rank}; }
        inline json remove_by_key_list(json keys, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_key_list, rt, keys}; }
        inline json remove_all_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_all_by_value, rt, value}; }
        inline json remove_by_value_list(json values, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_value_list, rt, values}; }
        inline json remove_by_key_interval(json key_start, json key_end, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_key_interval, rt, key_start, key_end}; }
        inline json remove_by_index_range(json index, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_index_range, rt, index, count}; }
        inline json remove_by_value_interval(json value_start, json value_end, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_value_interval, rt, value_start, value_end}; }
        inline json remove_by_rank_range(json rank, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_rank_range, rt, rank, count}; }
        inline json remove_by_key_rel_index_range(json key, json index, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_key_rel_index_range, rt, key, index, count}; }
        inline json remove_by_value_rel_rank_range(json value, json rank, json count, as_cdt::return_type rt = as_cdt::return_type::none) { return {as_cdt::map_op::remove_by_value_rel_rank_range, rt, value, rank, count}; }

        // Read operations
        inline json get_by_key(json key, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_key, rt, key}; }
        inline json get_by_index(json index, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_index, rt, index}; }
        inline json get_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_value, rt, value}; }
        inline json get_by_rank(json rank, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_rank, rt, rank}; }
        inline json get_all_by_value(json value, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_all_by_value, rt, value}; }
        inline json get_by_key_interval(json key_start, json key_end, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_key_interval, rt, key_start, key_end}; }
        inline json get_by_index_range(json index, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_index_range, rt, index, count}; }
        inline json get_by_value_interval(json value_start, json value_end, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_value_interval, rt, value_start, value_end}; }
        inline json get_by_rank_range(json rank, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_rank_range, rt, rank, count}; }
        inline json get_by_key_list(json keys, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_key_list, rt, keys}; }
        inline json get_by_value_list(json values, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_value_list, rt, values}; }
        inline json get_by_key_rel_index_range(json key, json index, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_key_rel_index_range, rt, key, index, count}; }
        inline json get_by_value_rel_rank_range(json value, json rank, json count, as_cdt::return_type rt = as_cdt::return_type::value) { return {as_cdt::map_op::get_by_value_rel_rank_range, rt, value, rank, count}; }
    }

    // Subcontext evaluation - opcode 255
    // Build subcontext evaluation operation
    // Returns: [255, context_array, operation]
    inline json subcontext_eval(
        const json& context_array,
        const json& operation
    ) {
        return {as_cdt::special_op::subcontext_eval, context_array, operation};
    }

    // CDT SELECT operation - opcode 254 (0xFE)
    //
    // IMPORTANT: Context behavior in SELECT operations
    // - Navigation contexts (map_key, list_index, map_rank, list_rank): Navigate INTO containers (select single element at each level)
    // - Filter contexts (AS_CDT_CTX_EXP): Filter ALL elements at a level using expression evaluation
    // - SELECT cannot be wrapped in an outer CDT context - it must be a top-level operation
    // - Context arrays can mix navigation and filter contexts: [map_key, "foo", list_index, 0, exp, <filter>]
    //
    // Selection modes determine output format:
    enum class select_mode : int
    {
        tree =              0,    // SELECT_TREE - Return matching elements WITH container structure preserved
                                  //   Example: {a: {b: [1,2,3]}} with ctx [map_key "a", map_key "b", exp >1]
                                  //   Returns: {a: {b: [2,3]}} (preserves nested map structure)
        leaf_list =         1,    // SELECT_LEAF_LIST - Extract values into flat list (no structure)
                                  //   Same example returns: [2, 3] (just the values)
        leaf_map_key =      2,    // SELECT_LEAF_MAP_KEY - Extract keys from matching map entries into flat list
        leaf_map_key_value = 3,    // SELECT_LEAF_MAP_KEY_VALUE - Extract [key, value] pairs from matching map entries
        apply =             4     // SELECT_APPLY - Modify selected elements in-place and return modified container
    };

    // Selection flags
    enum class select_flag : int
    {
        none =    0,
        no_fail = 0x10  // SELECT_NO_FAIL - Treat AS_EXP_UNK as AS_EXP_FALSE
    };

    // Build CDT select operation for read modes (tree, leaf_list, leaf_map_key)
    // Returns: [254, context_array, flags]
    inline json select(
        const json& context_array,
        select_mode mode,
        select_flag flags = select_flag::none
    ) {
        int64_t combined_flags = static_cast<int64_t>(mode) | static_cast<int64_t>(flags);
        return {as_cdt::special_op::select, context_array, combined_flags};
    }

    // Build CDT select operation for apply mode with transformation expression
    // Returns: [254, context_array, flags, apply_exp]
    inline json select_apply(
        const json& context_array,
        const json& apply_exp,
        select_flag flags = select_flag::none
    ) {
        int64_t combined_flags = static_cast<int64_t>(select_mode::apply) | static_cast<int64_t>(flags);
        return {as_cdt::special_op::select, context_array, combined_flags, apply_exp};
    }
}
