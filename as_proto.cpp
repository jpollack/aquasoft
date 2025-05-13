#include "as_proto.hpp"
#include <cstring>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
#include <chrono>

void as_header::size (size_t sz) {
    this->be_sz_extra = 0;
    this->be_sz = htobe32 (sz);
}

size_t as_header::size (void) const		{	return be32toh (this->be_sz); }

as_header::as_header (uint8_t _type, size_t _size) :
    version (2),
    type (_type) { this->size (_size); }

as_header::as_header (const as_msg* msg)	{	this->init (msg); }
as_header::as_header (const std::string& str)	{	this->init (str); }
as_header* as_header::init (const as_msg *msg)
{
    this->version = 2;
    this->type = 3;
    this->size ((msg->end () - msg->data) + 22);
    return this;
}

as_header* as_header::init (const std::string& str)
{
    this->version = 2;
    this->type = 1;
    this->size (str.length ());
    return this;
}

size_t	  as_field::data_sz	(void)	{	return (be32toh (this->be_sz) - 1);				}
as_field* as_field::next	(void)	{	return (as_field *) (this->data + this->data_sz ());		}

uint8_t*  as_op::data		(void)	{	return (uint8_t *) (this->name + this->name_sz);		}
size_t    as_op::data_sz	(void) const {	return (be32toh (this->be_sz) - (4 + this->name_sz));		}
as_op*    as_op::next		(void)	{	return (as_op *) (this->data () + this->data_sz ());		}
void	as_op::data_sz (size_t sz) {    this->be_sz = htobe32 (4 + this->name_sz + sz); }

uint16_t  as_msg::n_ops		(void) const {	return be16toh (this->be_ops);					}
uint16_t  as_msg::n_fields	(void) const {	return be16toh (this->be_fields);				}

void as_msg::clear (void)
{
    memset (&this->_res0, 0, sizeof(as_msg));
    this->_res0 = 22;
}

as_field *as_msg::field (as_field::type t) const
{
    as_field *f = (as_field *) this->data;

    for (auto ii = this->n_fields (); ii--; f = f->next ())
	if (f->t == t)
	    return f;

    return nullptr;
}

as_op* as_msg::ops_begin (void) const
{
    as_field *f = (as_field *) this->data;
    for (auto ii = this->n_fields (); ii--; f = f->next ());
    return (as_op *)f;
}

uint8_t* as_msg::end (void) const
{
    as_op *op = this->ops_begin ();
    for (auto ii = this->n_ops (); ii--; op = op->next ());
    return (uint8_t *)op;
}

as_field *as_msg::add (as_field::type t, size_t sz)
{
    if (this->be_ops)    return nullptr;

    as_field *f = (as_field *) this->data;
    for (auto ii = this->n_fields (); ii--; f = f->next ())
	if (f->t == t)
	    return nullptr;

    f->be_sz = htobe32 (sz + 1);
    f->t = t;
    this->be_fields = htobe16 (1 + this->n_fields ());
    return f;
}

as_field* as_msg::add (as_field::type type, size_t sz, const void *data) {
    return this->be_ops
	? nullptr
	: (as_field *) memcpy (this->add (type, sz)->data, data, sz);
}

as_field* as_msg::add (as_field::type type, const std::string& str) {
    return this->be_ops
	? nullptr
	: this->add (type, str.size (), str.c_str ());
}

as_op *as_msg::add (as_op::type type, size_t name_sz, size_t data_sz) {

    as_op *op = (as_op *)this->end ();
    memset (op, 0, sizeof (as_op));
    op->name_sz = (uint8_t) name_sz;
    op->data_sz (data_sz);
    op->op_type = type;
    this->be_ops = htobe16 (1 + this->n_ops ());
    return op;
}

as_op *as_msg::add (as_op::type type, const std::string& name, size_t data_sz, as_particle::type dt)
{
    as_op *op = this->add (type, name.size (), data_sz);
    memcpy (op->name, name.c_str (), name.size ());
    op->data_type = dt;
    return op;
}

as_op *as_msg::add (as_op::type type, const std::string& name, size_t data_sz, const void *data, as_particle::type dt)
{
    as_op *op = this->add (type, name, data_sz, dt);
    memcpy (op->data (), data, data_sz);
    return op;
}

as_op* as_msg::add (as_op::type type, const std::string& name, const std::string& val)
{
    return this->add (type, name, val.size (), val.c_str (), as_particle::type::t_string);
}

static size_t write (int fd, as_header hdr, const void* dptr)
{
    const struct iovec iov[2] = {
	{ .iov_base=&hdr,			.iov_len=8 },
	{ .iov_base=(void*)dptr,		.iov_len=hdr.size () }
    };
    return writev (fd, iov, 2);
}

size_t write (int fd, const std::string& str)
{
    as_header hdr (str);
    return write (fd, hdr, str.c_str ());
}

size_t write (int fd, const as_msg* msg)
{
    as_header hdr (msg);
    return write (fd, hdr, msg);
}

size_t read (int fd, void **obuf)
{
    uint64_t hb;
    as_header* hdr = (as_header *)&hb;

    if (read (fd, hdr, 8) != 8) {
	return 0;
    }

    size_t sz = hdr->size ();
    if (*obuf == nullptr) {
	*obuf = (char *)malloc (sz + 1);
	*((char*)*obuf + sz) = 0;
    }
    uint8_t *op = (uint8_t *) *obuf;
    while (sz) {
	auto gsz = read (fd, op, sz);
	op += gsz;
	sz -= gsz;
    }
    return op - (uint8_t *) *obuf;
}

size_t read (int fd, std::string& str)
{
    uint64_t hb;
    as_header* hdr = (as_header *)&hb;

    if (read (fd, hdr, 8) != 8) {
	return 0;
    }

    size_t sz = hdr->size ();
    str.resize (sz);
    char *op = &str[0];
    while (sz) {
	auto gsz = read (fd, op, sz);
	op += gsz;
	sz -= gsz;
    }
    return str.size ();
}

size_t call (int fd, void **obuf, const as_msg* msg, uint32_t *dur)
{
    auto tp0 = std::chrono::high_resolution_clock::now ();
    write (fd, msg);
    if (dur) {
	size_t sz{read (fd, obuf)};
	auto tp1 = std::chrono::high_resolution_clock::now ();
	*dur = (uint32_t) std::chrono::duration_cast<std::chrono::microseconds>(tp1 - tp0).count();
	return sz;
    } else
	return read (fd, obuf);
}

size_t call (int fd, as_msg **obuf, const as_msg* msg, uint32_t *dur)
{
    return call (fd, (void **) obuf, msg, dur);
}

size_t call (int fd, void **obuf, const std::string& str, uint32_t *dur)
{
    auto tp0 = std::chrono::high_resolution_clock::now ();
    write (fd, str);
    if (dur) {
	size_t sz{read (fd, obuf)};
	auto tp1 = std::chrono::high_resolution_clock::now ();
	*dur = (uint32_t) std::chrono::duration_cast<std::chrono::microseconds>(tp1 - tp0).count();
	return sz;
    } else
	return read (fd, obuf);
}

size_t call_info (int fd, std::string& obuf, const std::string& ibuf, uint32_t *dur)
{
    auto tp0 = std::chrono::high_resolution_clock::now ();
    write (fd, ibuf);
    if (dur) {
	size_t sz{read (fd, obuf)};
	auto tp1 = std::chrono::high_resolution_clock::now ();
	*dur = (uint32_t) std::chrono::duration_cast<std::chrono::microseconds>(tp1 - tp0).count();
	return sz;
    } else
	return read (fd, obuf);
}

std::string call_info (int fd, const std::string& str, uint32_t *dur)
{
    std::string ret;
    call_info (fd, ret, str, dur);
    // remove the trailing newline
    ret.resize (ret.size () - 1);
    return ret;
}

std::string to_string (const as_field::type t)
{
    switch (t)
    {
    case(as_field::type::t_namespace):			return "namespace";
    case(as_field::type::t_set):			return "set";
    case(as_field::type::t_key):			return "key";
    case(as_field::type::t_record_version):		return "record_version";
    case(as_field::type::t_digest_ripe):		return "digest_ripe";
    case(as_field::type::t_mrtid):			return "mrtid";
    case(as_field::type::t_mrt_deadline):		return "mrt_deadline";
    case(as_field::type::t_trid):			return "trid";
    case(as_field::type::t_socket_timeout):		return "socket_timeout";
    case(as_field::type::t_recs_per_sec):		return "recs_per_sec";
    case(as_field::type::t_pid_array):			return "pid_array";
    case(as_field::type::t_digest_array):		return "digest_array";
    case(as_field::type::t_sample_max):			return "sample_max";
    case(as_field::type::t_lut):			return "lut";
    case(as_field::type::t_bval_array):			return "bval_array";
    case(as_field::type::t_index_name):			return "index_name";
    case(as_field::type::t_index_range):		return "index_range";
    case(as_field::type::t_index_context):		return "index_context";
    case(as_field::type::t_index_type):			return "index_type";
    case(as_field::type::t_udf_filename):		return "udf_filename";
    case(as_field::type::t_udf_function):		return "udf_function";
    case(as_field::type::t_udf_arglist):		return "udf_arglist";
    case(as_field::type::t_udf_op):			return "udf_op";
    case(as_field::type::t_query_binlist):		return "query_binlist";
    case(as_field::type::t_batch):			return "batch";
    case(as_field::type::t_batch_with_set):		return "batch_with_set";
    case(as_field::type::t_predexp):			return "predexp";
    case(as_field::type::t_conndata):			return "conndata";
    }
    return "unknown";
}

std::string to_string (const as_op::type t)
{
    switch (t)
    {
    case(as_op::type::t_none):			return "none";
    case(as_op::type::t_read):			return "read";
    case(as_op::type::t_write):			return "write";
    case(as_op::type::t_cdt_read):			return "cdt_read";
    case(as_op::type::t_cdt_modify):			return "cdt_modify";
    case(as_op::type::t_incr):			return "incr";
    case(as_op::type::t_exp_read):			return "exp_read";
    case(as_op::type::t_exp_modify):			return "exp_modify";
    case(as_op::type::t_append):			return "append";
    case(as_op::type::t_prepend):			return "prepend";
    case(as_op::type::t_touch):			return "touch";
    case(as_op::type::t_bits_read):			return "bits_read";
    case(as_op::type::t_bits_modify):		return "bits_modify";
    case(as_op::type::t_delete_all):			return "delete_all";
    case(as_op::type::t_hll_read):			return "hll_read";
    case(as_op::type::t_hll_modify):			return "hll_modify";
    }
    return "unknown";
}
