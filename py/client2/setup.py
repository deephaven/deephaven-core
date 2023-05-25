from setuptools import Extension, setup
from Cython.Build import cythonize

setup(
    ext_modules = cythonize(
        [Extension("deephaven_client",
                   ["deephaven_client.pyx"],
# not sure the correct way to select C++17
                   extra_compile_args=["-std=c++17"],
                   libraries=["client", "protobufd", "arrow_flight", "arrow", "arrow_bundled_dependencies", "grpc++", "grpc", "address_sorting", "gpr", "upb", "absl_raw_hash_set", "absl_hashtablez_sampler", "absl_exponential_biased", "absl_hash", "absl_city", "absl_wyhash", "absl_statusor", "absl_bad_variant_access", "/home/kosak/dhcpp/local/grpc/lib/libgpr", "/home/kosak/dhcpp/local/grpc/lib/libupb", "absl_status", "absl_random_distributions", "absl_random_seed_sequences", "absl_random_internal_pool_urbg", "absl_random_internal_randen", "absl_random_internal_randen_hwaes", "absl_random_internal_randen_hwaes_impl", "absl_random_internal_randen_slow", "absl_random_internal_platform", "absl_random_internal_seed_material", "absl_random_seed_gen_exception", "absl_cord", "absl_bad_optional_access", "absl_str_format_internal", "absl_synchronization", "absl_stacktrace", "absl_symbolize", "absl_debugging_internal", "absl_demangle_internal", "absl_graphcycles_internal", "absl_malloc_internal", "absl_time", "absl_strings", "absl_throw_delegate", "absl_int128", "absl_strings_internal", "absl_base", "absl_raw_logging_internal", "absl_log_severity", "absl_spinlock_wait", "absl_civil_time", "absl_time_zone", "ssl", "re2", "cares"]
        )])
)

