load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

all_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

def node_repositories():
    maybe(
        http_archive,
        name = "cpptoml",
        strip_prefix = "cpptoml-0.1.1",
        sha256 = "23af72468cfd4040984d46a0dd2a609538579c78ddc429d6b8fd7a10a6e24403",
        urls = ["https://boss.hdslb.com/common-arch/dist/cpptoml-v0.1.1.tar.gz"],
        build_file = "//:third_party/cpptoml.BUILD",
    )
    maybe(
        http_archive,
        name = "com_github_fmtlib_fmt",
        build_file = "//:third_party/fmt.BUILD",
        sha256 = "5d98c504d0205f912e22449ecdea776b78ce0bb096927334f80781e720084c9f",
        strip_prefix = "fmt-7.1.3",
        urls = ["https://boss.hdslb.com/common-arch/dist/fmt-7.1.3.zip"],
    )
    maybe(
        http_archive,
        name = "com_github_brpc_brpc",
        urls = ["https://boss.hdslb.com/common-arch/dist/brpc-1.9.0.tar.gz"],
        strip_prefix = "brpc-1.9.0",
        sha256 = "85856da0216773e1296834116f69f9e80007b7ff421db3be5c9d1890ecfaea74",
    )
    maybe(
        http_archive,
        name = "com_github_google_leveldb",
        build_file = "//:third_party/leveldb.BUILD",
        sha256 = "f5abe8b5b209c2f36560b75f32ce61412f39a2922f7045ae764a2c23335b6664",
        strip_prefix = "leveldb-1.20",
        url = "https://boss.hdslb.com/common-arch/dist/leveldb-v1.20.tar.gz",
    )
    maybe(
        http_archive,
        name = "openssl",
        build_file = "//:third_party/openssl.BUILD",
        sha256 = "f89199be8b23ca45fc7cb9f1d8d3ee67312318286ad030f5316aca6462db6c96",
        strip_prefix = "openssl-1.1.1m",
        urls = [
                "https://boss.hdslb.com/common-arch/dist/openssl-1.1.1m.tar.gz",
        ],
    )
    maybe(
        http_archive,
        name = "rules_foreign_cc",
        sha256 = "bdfc2734367a1242514251c7ed2dd12f65dd6d19a97e6a2c61106851be8e7fb8",
        strip_prefix = "rules_foreign_cc-master",
        url = "https://boss.hdslb.com/common-arch/dist/rules_foreign_cc-master.zip",
    )
    maybe(
        http_archive,
        name = "net_zlib_zlib",
        build_file = "//:third_party/zlib.BUILD",
        sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
        strip_prefix = "zlib-1.2.11",
        url = "https://boss.hdslb.com/common-arch/dist/zlib-1.2.11.tar.gz",
    )
    maybe(
        http_archive,
        name = "xxhash",
        build_file = "//:third_party/xxhash.BUILD",
        sha256 = "baee0c6afd4f03165de7a4e67988d16f0f2b257b51d0e3cb91909302a26a79c4",
        strip_prefix = "xxHash-0.8.2",
        url = "https://boss.hdslb.com/common-arch/dist/xxHash-0.8.2.tar.gz",
    )
    maybe(
        http_archive,
        name = "com_github_jupp0r_prometheus_cpp",
        sha256 = "70344acaa89912fd2027408f0e382ba8cb65cfc268e231e66ce1fa7fcc4c0963",
        strip_prefix = "prometheus-cpp-master",
        url = "https://boss.hdslb.com/common-arch/dist/prometheus-cpp-1.1.0.zip",
    )

def _data_deps_extension_impl(ctx):
    node_repositories()

data_deps_ext = module_extension(
    implementation = _data_deps_extension_impl,
)
